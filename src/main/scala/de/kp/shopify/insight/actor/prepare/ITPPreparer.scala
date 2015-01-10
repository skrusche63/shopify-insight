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
 * Customer segmentation is the first building block of any marketing strategy. The basic
 * idea behind segmentation is to divide customers into groups such that within a group,
 * customers arevery similar based on selected attributes. Product affinity segmentation 
 * refers to targeting segmentation and is done using transactional attributes and not
 * all customers may be included in targeting segments.
 * 
 * Product affinity is the natural liking of customers for products, and product affinity
 * segmentation divides customers into groups based on purchased products. Such segments
 * often suffer from the fact that there is one large cluster and many tiny ones.
 * 
 * This results from the fact, that in most cases (stores), there are few customers who buy
 * a lot in a product class while most others buy little. Such patterns often create major
 * problems in segmentation because the commonly used clustering algorithms perform poorly
 * in the presence of such extreme skewness and high kurtosis.
 * 
 * Of course, the recommended solution is to bring the data shape back to a normal (uniform)
 * distribution.
 * 
 * Product level data for a customer often has a lot of zero values in many product classes.
 * Large numbers of zero values pose another problem both methodologically (from clustering
 * algorithm's ability to separate groups) and substantively (once we found segments, how
 * should those segments be interpreted).
 * 
 * The preparer of the product affinity profile (PAP) addresses this problem and aims to
 * prepare product level purchase frequency such that clustering results in almost equal
 * sized clusters.
 * 
 * There different segmentation approaches to address product affinity segments, and from
 * our experience the clustering one ist the most powerful and profitable information for
 * marketing campains and communications with customers.
 * 
 *   
 */
class ITPPreparer(requestCtx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor(requestCtx) {
  
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
        
        val table = orders.groupBy(x => (x.site,x.user)).flatMap(x => {
          
          val (site,user) = x._1          
          val total = x._2.size
          
          val s0 = x._2.flatMap(_.items.map(v => (v.item,v.quantity)))
          val s1 = s0.groupBy(_._1)
          
          val supp = s1.map(v => (v._1, v._2.map(_._2).sum))      
          val pref = supp.map(v => (v._1,Math.log(1 + v._2.toDouble / total.toDouble)))
          
          supp.map(x => ParquetITP(site,user,x._1,x._2,pref(x._1),total))
          
          
        })
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/ITP/%s""",requestCtx.getBase,uid)         
        table.saveAsParquetFile(store)

        val params = Map(Names.REQ_MODEL -> "ITP") ++ req_params
        context.parent ! PrepareFinished(params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] ITP preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  private def quantiles(dataset:RDD[Double]):Map[Double,Double] = {
    
    implicit val semigroup = new QTreeSemigroup[Double](K)
    
    val qtree = dataset.map(v => QTree(v)).reduce(_ + _) 
    QUANTILES.map(x => {
      
      val (lower,upper) = qtree.quantileBounds(x)
      val mean = (lower + upper) / 2

      (x,mean)
      
    }).toMap
    
  }
  
}