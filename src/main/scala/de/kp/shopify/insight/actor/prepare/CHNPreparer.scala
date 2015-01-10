package de.kp.shopify.insight.actor.prepare
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

import com.twitter.algebird._
import com.twitter.algebird.Operators._

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.actor.BaseActor

/**
 * The CHNPreparer generates the timespan distribution in terms of
 * days, and the amount distribution from all orders registered so 
 * far, excludes the last transaction and determines whether this
 * last transaction is far away from the "normal" behavior of the
 * respective customer
 */
class CHNPreparer(requestCtx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor(requestCtx) {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  /*
   * The parameter K is used as an initialization prameter for the 
   * QTree semigroup that is used to compute quantile boundaries
   * for the amounts spent and also for the timespans elapsed 
   * between two subsequent transactions
   */
  private val K = 6
  
  /*
   * The CHNPreparer uses thresholds for the quantile calculation with
   * respect to the amount spent by the customer and the elapsed time 
   * span between two susequent transactions.
   * 
   * The "normal" customer behavior is defined as those data points that
   * are above the amount threshold, and below the timespan threshold
   */
  private val AMOUNT_THRESHOLD   = 0.1
  private val TIMESPAN_THRESHOLD = 0.9
        
  import sqlc.createSchemaRDD
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data      
      val uid = req_params(Names.REQ_UID)
      
      try {

        val s0 = orders.map(x => (x.site,x.user,x.amount,x.timestamp)).groupBy(x => (x._1,x._2)).filter(_._2.size > 1)
        val table = s0.map(x => {
          
          val (site,user) = x._1
          /*
           * STEP #1: Prepare customer-specific data with 
           * respect to amount spent and timespans in terms
           * of days
           */
          val data = x._2.toSeq.sortBy(_._4)
          
          val amounts = data.map(_._3)

          val timestamps = data.map(_._4)
          val timespans = timestamps.zip(timestamps.tail).map(v => v._2 - v._1).map(v => (if (v / DAY < 1) 1 else v / DAY))
          
          /*
           * STEP #2: Build churn boundary from all amounts
           * except the last one and determine whether the
           * last amount spent is below the 10% boundary of
           * all previous purchases
           */
          val init_amounts = amounts.init.map(_.toDouble).sorted
          val amount_churn = if (init_amounts.size > 1) {
            
            val boundary = computeBoundary(init_amounts,AMOUNT_THRESHOLD)
            if (amounts.last.toDouble < boundary) true else false
              
          } else false
          
          /*
           * STEP #3: Build churn boundary from all timespans
           * except the last one and determine whether the last
           * timespan is above the 90% boundary of all previous
           * purchases.
           */
          val init_timespans = timespans.init.map(_.toDouble).sorted
          val timespan_churn = if (init_timespans.size > 1) {
            
            val boundary = computeBoundary(init_timespans,TIMESPAN_THRESHOLD)
            if (timespans.last.toDouble > boundary) true else false
            
          } else false
          
          val churner = amount_churn && timespan_churn
          
          ParquetCHN(
              site,
              user,
              amounts.last,
              timespans.last,
              churner
          )
          
        })
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/CHN/%s""",requestCtx.getBase,uid)         
        table.saveAsParquetFile(store)

        val params = Map(Names.REQ_MODEL -> "CHN") ++ req_params
        context.parent ! PrepareFinished(params)
        
      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] CHN preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
  private def computeBoundary(dataset:Seq[Double],quantile:Double):Double = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d1 = dataset.map(v => QTree(v)).reduce(_ + _) 
    
    val (lower,upper) = d1.quantileBounds(quantile)
    val mean = (lower + upper) / 2
    
    mean
    
  }

}