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
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import com.twitter.algebird._
import com.twitter.algebird.Operators._

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.analytics.StateHandler
import de.kp.shopify.insight.actor.BaseActor

/**
 * The STMPreparer generates a state representation for all customers
 * that have purchased at least twice since the start of the collection
 * of the Shopify orders.
 * 
 * Note, that we actually do not distinguish between customers that have 
 * a more frequent purchase behavior, and those, that have purchased only
 * twice.
 * 
 */
class STMPreparer(requestCtx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor(requestCtx) {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  /*
   * The parameter K is used as an initialization 
   * prameter for the QTree semigroup
   */
  private val K = 6
  private val QUANTILES = List(0.20,0.40,0.60,0.80,1.00)
  
  import sqlc.createSchemaRDD
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      val uid = req_params(Names.REQ_UID)
      
      try {

        /*
         * STEP #1: We calculate the amount ratios and timespans from
         * subsequent customer specific purchase transactions and then
         * do a quantile analysis to find univariate boundaries 
         */
        val rawset = orders.groupBy(x => (x.site,x.user)).filter(_._2.size > 1).map(x => {
      
          /* Compute time ordered list of (amount,timestamp) */
          val data = x._2.map(v => (v.amount,v.timestamp)).toList.sortBy(_._2)      

          val amounts = data.map(_._1)       
          val ratios = amounts.zip(amounts.tail).map(v => v._2 / v._1)

          val timestamps = data.map(_._2)
          val timespans = timestamps.zip(timestamps.tail).map(v => v._2 - v._1).map(v => (if (v / DAY < 1) 1 else v / DAY))

          (ratios,timespans)
          
        })
       
        val ratios = rawset.flatMap(_._1).sortBy(x => x)
        val r_quantiles = sc.broadcast(quantiles(ratios))
        
        val spans = rawset.flatMap(_._2).map(_.toDouble).sortBy(x => x)
        val s_quantiles = sc.broadcast(quantiles(spans))
        
        val table = orders.groupBy(x => (x.site,x.user)).filter(_._2.size > 1).flatMap(x => {

          val (site,user) = x._1  
      
          /* Compute time ordered list of (amount,timestamp) */
          val data = x._2.map(v => (v.amount,v.timestamp)).toList.sortBy(_._2)      

          val amounts = data.map(_._1)    
          val amount_ratios = amounts.zip(amounts.tail).map(v => v._2 / v._1)
          
          /*
           * We introduce a rating from 1..5 for the amount ratio attribute
           * and assign 5 to the highest value, 4 to a less valuable etc
           */     
          val r_b1 = r_quantiles.value(0.20)
          val r_b2 = r_quantiles.value(0.40)
          val r_b3 = r_quantiles.value(0.60)
          val r_b4 = r_quantiles.value(0.80)
          val r_b5 = r_quantiles.value(1.00)
          
          val amount_states = amount_ratios.map(ratio => {
            
            val rval = (
              if (ratio < r_b1) 1
              else if (r_b1 <= ratio && ratio < r_b2) 2
              else if (r_b2 <= ratio && ratio < r_b3) 3
              else if (r_b3 <= ratio && ratio < r_b4) 4
              else if (r_b4 <= ratio) 5
              else 0  
            )

            if (rval == 0) throw new Exception("rval = 0 is not support.")
            rval.toString
            
          })

          val timestamps = data.map(_._2)
          val timespans = timestamps.zip(timestamps.tail).map(v => (if ((v._2 - v._1) / DAY < 1) 1.toInt else ((v._2 - v._1) / DAY)).toInt)

          /*
           * We introduce a rating from 1..5 for the timespan attribute
           * and assign 5 to the lowest value, 4 to a higher valuable etc
           */     
          val s_b1 = s_quantiles.value(0.20)
          val s_b2 = s_quantiles.value(0.40)
          val s_b3 = s_quantiles.value(0.60)
          val s_b4 = s_quantiles.value(0.80)
          val s_b5 = s_quantiles.value(1.00)

          val timespan_states = timespans.map(span => {
            
            val sval = (
              if (span < s_b1) 5
              else if (s_b1 <= span && span < s_b2) 4
              else if (s_b2 <= span && span < s_b3) 3
              else if (s_b3 <= span && span < s_b4) 2
              else if (s_b4 <= span) 1
              else 0  
            )

            if (sval == 0) throw new Exception("sval = 0 is not support.")
            sval.toString
            
          })

          val states = amount_states.zip(timespan_states).map(x => x._1 + x._2)
          /*
           * Note, that amount ratios, timespans, amount & timestamp tail
           * and states are ordered appropriately
           */
          val ds = amounts.tail.zip(timestamps.tail).zip(states)         
          ds.map(v => {
          
            val ((amount, timestamp), state) = v
            ParquetSTM(
                site,
                user,
                amount,
                timestamp,
                /*
                 * The quantile boundaries are provided here
                 * in a de-normalized manner to enable quick
                 * forecast modeling after having determined
                 * the state predictions
                 */
                r_b1,
                r_b2,
                r_b3,
                r_b4,
                r_b5,
                s_b1,
                s_b2,
                s_b3,
                s_b4,
                s_b5,
                state
            )

          })
        })
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/STM/%s""",requestCtx.getBase,uid)         
        table.saveAsParquetFile(store)

        val params = Map(Names.REQ_MODEL -> "STM") ++ req_params
        context.parent ! PrepareFinished(params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] STM preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
  private def quantiles(rawset:RDD[Double]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val qtree = rawset.map(v => QTree(v)).reduce(_ + _) 
    QUANTILES.map(x => {
      
      val (lower,upper) = qtree.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
}