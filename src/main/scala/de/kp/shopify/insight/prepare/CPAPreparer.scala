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

import de.kp.shopify.insight.preference.TFIDF

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
 * distribution. This is why, we use the TFIDF algorithm to assign product affinity score,
 * and also perform z-score normalization when clustering the labeled feature vectors.
 * 
 * Bote, that there exist different segmentation approaches to address product affinity 
 * segments, and from our experience the clustering one ist the most powerful and profitable 
 * information for marketing campains and communications with customers.
 *   
 */
class CPAPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
  
  import sqlc.createSchemaRDD
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      
      val uid = req_params(Names.REQ_UID)
      val customer = req_params("customer").toInt
             
      val start = new java.util.Date().getTime.toString            
      ctx.listener ! String.format("""[INFO][UID: %s] CPA preparation request received at %s.""",uid,start)
      
      try {
        /*
         * STEP #1: Restrict the purchase orders to those items and attributes
         * that are relevant for the item segmentation task; this encloses a
         * filtering with respect to customer type, if different from '0'
         */
        val ctype = sc.broadcast(customer)

        val ds = orders.flatMap(x => x.items.map(v => (x.site,x.user,v.item,v.quantity,v.category)))
        val filteredDS = (if (customer == 0) {
          /*
           * This customer type indicates that ALL customer types
           * have to be taken into account when computing the item
           * segmentation 
           */
          ds
          
        } else {
          /*
           * Load the Parquet file that specifies the customer type specification 
           * and filter those customers that match the provided customer type
           */
          val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
          ds.map(x => ((x._1,x._2),(x._3,x._4,x._5))).join(parquetCST).map(x => {
            
            val ((site,user),((item,quantity,category),rfm_type)) = x
            (site,user,item,quantity,category)
            
          })
        })     

        /*
         * STEP #2: Compute the customer product affinity (CPA) using the 
         * TDIDF algorithm from text analysis
         */
        val table = TFIDF.computeCPA(filteredDS)
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/CPA-%s/%s""",ctx.getBase,customer.toString,uid)         
        table.saveAsParquetFile(store)

        val end = new java.util.Date().getTime
        ctx.listener ! String.format("""[INFO][UID: %s] CPA preparation for customer type '%s' finished at %s.""",uid,customer.toString,end.toString)

        val params = Map(Names.REQ_MODEL -> "CPA") ++ req_params
        context.parent ! PrepareFinished(params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          ctx.listener ! String.format("""[ERROR][UID: %s] CPA preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
}