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
import org.joda.time.DateTime

import com.twitter.algebird._
import com.twitter.algebird.Operators._

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.actor.BaseActor
/**
 * The RFM Customer Segmentation model is an embarrassingly simple way of 
 * segmenting the customer base inside a marketing database. The resulting 
 * groups are easy to understand, analyze and action without the need of 
 * going through complex mathematics.
 * 
 */
class RFMPreparer(requestCtx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  /*
   * The parameter K is used as an initialization 
   * prameter for the QTree semigroup
   */
  private val K = 6
  private val QUANTILES = List(0.25,0.50,0.75,1.00)
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data      
      val uid = req_params(Names.REQ_UID)
      
      try {

        /*
         * The first step in RFM Customer Segmentation is to define the three attributes. 
         * The model allows for a certain flexibility with definitions and you can adjust 
         * them to the specifics of your business. 
         * 
         * The three attributes are:
         * 
         * - Recency which represents the “freshness” of customer activity. Naturally, we 
         *   would like to identify active and inactive customers. The basic definition for 
         *   this attribute is the number of days since last order or interaction.
         * 
         * - Frequency captures how often the customer buys. By default this could be the 
         *   total number of orders in the last year or during the whole of customer’s lifetime.
         * 
         * - Monetary value indicates how much the customer is spending. In many cases this 
         *   is just the sum of all order values.
         *   
         */
        val sc = requestCtx.sparkContext
        val rawset = orders.groupBy(x => (x.site,x.user)).map(p => {

          val (site,user) = p._1  
      
          /* Compute time ordered list of (amount,timestamp) */
          val s0 = p._2.map(x => (x.amount,x.timestamp)).toList.sortBy(_._2)      

          val today = new DateTime().getMillis
          val R = ((s0.map(_._2).last - today) / DAY).toInt
          /*
           * Frequency is the total number of orders made by
           * a certain customer during the whole lifetime
           */
          val F = s0.size 
          /* 
           * Monetary attribute is calculated as the sum
           * of all order values 
           */
          val M = s0.map(_._1).sum
          
          (site,user,today,R,F,M)

        })
        
        /*
         * Now that the RFM rawset is ready, we allocate the respective attribute values
         * into segments. To this end, we use quantiles. Simply put, this means folllowing:
         * 
         * The k-th quantile is a value x such that k% of values are less than x. So if the 
         * 25th quantile is 10, then 25% of all values are smaller than 10. 
         * 
         * As we try to compute valulabe data as early as possible, the RFM table is used
         * to calulcate the quantiles for the different attributes and the result are
         * re-assigned.
         */
        val r_quantiles = sc.broadcast(RQuantiles(rawset))
        val f_quantiles = sc.broadcast(FQuantiles(rawset))
 
        val m_quantiles = sc.broadcast(MQuantiles(rawset))
        
        val table = rawset.map(x => {
          
          val (site,user,today,r,f,m) = x

          /********** RECENCY ***********/
      
          /*
           * We use the quantiles description to divide
           * all the users into three segments:
           * 
           * a) high value::    0 < value < 0.25 boundary
           * b) medium value:: 0.25 boundary <= value < 0.75 boundary
           * c) low value::   0.75 boundary <= value
           */
          val r_b1 = r_quantiles.value(0.25)
          val r_b2 = r_quantiles.value(0.75)
      
          val r_segment = (
            if (r < r_b1) "H"
            else if (r_b1 <= r && r < r_b2) "M"
            else if (r_b2 <= r) "L"
            else "-"  
          )
          
          val r_quantiles_str = r_quantiles.value.map(v => String.format("""%s:%s""",v._1.toString,v._2.toString)).mkString(",")
          
          
          /********** FREQUENCY *********/
      
          /*
           * We use the quantiles description to divide
           * all the users into three segments:
           * 
           * a) low value::    0 < value < 0.25 boundary
           * b) medium value:: 0.25 boundary <= value < 0.75 boundary
           * c) high value::   0.75 boundary <= value
           */
          val f_b1 = f_quantiles.value(0.25)
          val f_b2 = f_quantiles.value(0.75)
      
          val f_segment = (
            if (f < f_b1) "L"
            else if (f_b1 <= f && f < f_b2) "M"
            else if (f_b2 <= f) "H"
            else "-"  
          )
          
          val f_quantiles_str = f_quantiles.value.map(v => String.format("""%s:%s""",v._1.toString,v._2.toString)).mkString(",")

          /********** MONETARY **********/
          
          /*
           * We use the quantiles description to divide
           * all the users into three segments:
           * 
           * a) low value::    0 < value < 0.25 boundary
           * b) medium value:: 0.25 boundary <= value < 0.75 boundary
           * c) high value::   0.75 boundary <= value
           */
          val m_b1 = m_quantiles.value(0.25)
          val m_b2 = m_quantiles.value(0.75)
      
          val m_segment = (
            if (m < m_b1) "L"
            else if (m_b1 <= m && m < m_b2) "M"
            else if (m_b2 <= m) "H"
            else "-"  
          )
          
          val m_quantiles_str = m_quantiles.value.map(v => String.format("""%s:%s""",v._1.toString,v._2.toString)).mkString(",")
          
          ParquetRFM(
              site,
              user,
              today,
              r,
              r_segment,
              r_quantiles_str,
              f,
              f_segment,
              f_quantiles_str,
              m,
              m_segment,
              m_quantiles_str
           )
        
        })
        
        val sqlCtx = new SQLContext(sc)
        import sqlCtx.createSchemaRDD

        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/RFM/%s""",requestCtx.getBase,uid)         
        table.saveAsParquetFile(store)

        val params = Map(Names.REQ_MODEL -> "RFM") ++ req_params
        context.parent ! PrepareFinished(params)
        
      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] RFM preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
  private def RQuantiles(dataset:RDD[(String,String,Long,Int,Int,Float)]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d0 = dataset.map(_._4.toDouble).collect.toSeq.sorted
    val d1 = d0.map(v => QTree(v)).reduce(_ + _) 

    QUANTILES.map(x => {
      
      val (lower,upper) = d1.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
  private def FQuantiles(dataset:RDD[(String,String,Long,Int,Int,Float)]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d0 = dataset.map(_._5.toDouble).collect.toSeq.sorted
    val d1 = d0.map(v => QTree(v)).reduce(_ + _) 

    QUANTILES.map(x => {
      
      val (lower,upper) = d1.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
  private def MQuantiles(dataset:RDD[(String,String,Long,Int,Int,Float)]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val d0 = dataset.map(_._6).collect.toSeq.sorted
    val d1 = d0.map(v => QTree(v)).reduce(_ + _) 

    QUANTILES.map(x => {
      
      val (lower,upper) = d1.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
}