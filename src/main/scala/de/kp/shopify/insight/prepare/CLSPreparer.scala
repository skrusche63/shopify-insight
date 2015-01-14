package de.kp.shopify.insight.prepare
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Shopify-Insight project
* (https://github.com/skrusche63/shopify-insight).
* 
* Shopify-Insight is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Shopify-Insight is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Shopify-Insight. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

class CLSPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
  
  /*
   * The CCNPreparer uses thresholds for the quantile calculation with
   * respect to the amount spent by the customer and the elapsed time 
   * span between two susequent transactions.
   * 
   * The "normal" customer behavior is defined as those data points that
   * are above the amount threshold, and below the timespan threshold
   */
  private val AMOUNT_THRESHOLD   = 0.1
  private val TIMESPAN_THRESHOLD = 0.9

  private val QUINTILES = List(0.10,0.40,0.60,0.90,1.00)

  import sqlc.createSchemaRDD
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data      
      
      val uid = req_params(Names.REQ_UID)
      val customer = req_params("customer").toInt
             
      val start = new java.util.Date().getTime.toString            
      ctx.listener ! String.format("""[INFO][UID: %s] CLS preparation request received at %s.""",uid,start)
      
      try {

        val ds = orders.map(x => (x.site,x.user,x.amount,x.timestamp)).groupBy(x => (x._1,x._2)).filter(_._2.size > 1)
        val ds1 = ds.map(x => {
          
          val (site,user) = x._1
          /*
           * STEP #1: Prepare customer-specific data with respect to 
           * the amount spent and timespans in terms of days
           */
          val data = x._2.toSeq.sortBy(_._4)
          
          val amounts = data.map(_._3)

          val timestamps = data.map(_._4)
          val timespans = timestamps.zip(timestamps.tail).map(v => v._2 - v._1).map(v => (if (v / DAY < 1) 1 else v / DAY).toInt)
          
          /*
           * STEP #2: Build boundaries from all amounts except the last 
           * one and describe the last amount in terms of 4 segments,
           * 
           * 1: Fade off
           * 2: Vulnerable
           * 3: Neutral
           * 4: Loyal
           * 
           */
          val init_amounts = amounts.init.map(_.toDouble).sorted
          val last_amount = amounts.last
          
          val m_b1 = boundary(init_amounts,0.1)
          val m_b2 = boundary(init_amounts,0.4)
          val m_b3 = boundary(init_amounts,0.7)
          
          val mval = (
            if (last_amount < m_b1) 1
            else if (m_b1 <= last_amount && last_amount < m_b2) 2
            else if (m_b2 <= last_amount && last_amount < m_b3) 3
            else 4
          )
          
          /*
           * STEP #3: Build boundaries from all timespans except the last 
           * one and describe the last timespan in terms of 4 segments,
           * 
           * 1: Fade off
           * 2: Vulnerable
           * 3: Neutral
           * 4: Loyal
           * 
           */
          val init_timespans = timespans.init.map(_.toDouble).sorted
          val last_timespan = timespans.last.toDouble
          
          val t_b1 = boundary(init_amounts,0.3)
          val t_b2 = boundary(init_amounts,0.6)
          val t_b3 = boundary(init_amounts,0.9)
          
          val tval = (
            if (last_timespan < t_b1) 4
            else if (t_b1 <= last_timespan && last_timespan < t_b2) 3
            else if (t_b2 <= last_timespan && last_timespan < t_b3) 2
            else 1
          )
          
          /*
           * STEP #4: From the individual loyalty segments for last amount 
           * and recency, we compute the overall loyalty state. To this end,
           * we use the following segmentation approach from the 16 possible
           * combinations:
           * 
           * 1: 11
           * 2: 12,13,14,21,22,31,41 -> (score) 12,13,13,22
           * 3: 23,24,32,33,42       -> (score) 23,23,33
           * 4: 34,43,44             -> (score) 34,44
           */
          
          val score = (
            if (mval < tval) 10*mval + tval
            else if (tval < mval) 10*tval + mval
            else 10*mval + tval
          )
          
          val loyalty = (
            if (score == 11) 1
            /* (1,2) (1,3) (1,4) (2,2) */
            else if (11 < score && score <= 22) 2
            /* (2,3) (2,4) (3,3) */
            else if (22 < score && score <= 33) 3
            /* (3,4) (4,4) */
            else 4
          )
 
          ((site,user,amounts.last,timespans.last,loyalty))
          
        })
        /*
         * Step #5: Load the Parquet file that specifies the customer type 
         * specification and join with the loyalty data computed so far
         */
        val parquetCST = readCST(uid)      
        val table = ds1.map(x => ((x._1,x._2),(x._3,x._4,x._5))).join(parquetCST).map(x => {
            
          val ((site,user),((amount,timespan,loyalty),rfm_type)) = x
          ParquetCLS(site,user,amount,timespan,loyalty,rfm_type)
            
        })
         
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/CLS/%s""",ctx.getBase,uid)         
        table.saveAsParquetFile(store)

        val end = new java.util.Date().getTime
        ctx.listener ! String.format("""[INFO][UID: %s] CLS preparation finished at %s.""",uid,end.toString)

        val params = Map(Names.REQ_MODEL -> "CLS") ++ req_params
        context.parent ! PrepareFinished(params)
        
      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          ctx.listener ! String.format("""[ERROR][UID: %s] CLS preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }

}