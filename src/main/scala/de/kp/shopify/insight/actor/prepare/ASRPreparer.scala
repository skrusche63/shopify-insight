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

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

/**
 * The ASRPreparer prepares the purchase transactions of a certain
 * period of time and a specific customer type for association rule
 * mining with Predictiveworks' Association Analysis engine
 */
class ASRPreparer(requestCtx:RequestContext,customer:Int,orders:RDD[InsightOrder]) extends BasePreparer(requestCtx) {

  import sqlc.createSchemaRDD
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data      
      val uid = req_params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      requestCtx.listener ! String.format("""[INFO][UID: %s] ASR preparation request received at %s.""",uid,start)
      
      try {
        /*
         * Association rule mining determines items that are frequently
         * bought together; to this end, we filter those transactions
         * with more than one item purchased 
         */
        val ds = orders.filter(x => x.items.size > 1).flatMap(x => x.items.map(v => (x.site,x.user,x.group,v.item)))
        /*
         * STEP #1: Restrict the purchase orders to those records that match
         * the provided customer type. In case of customer type = '0', the 
         * 'user' provided with the ParquetASR table is the customer itself.
         * 
         * In all other cases, the 'user' attribute is replaced by the customer
         * type and the respective transactions are assigned to this type
         */
        val ctype = sc.broadcast(customer)
        val table = (if (customer == 0) {
          /*
           * This customer type indicates that ALL customer types
           * have to be taken into account when computing the item
           * segmentation 
           */
          ds.map(x => ParquetASR(x._1,x._2,x._3,x._4))
          
        } else {
          /*
           * Load the Parquet file that specifies the customer type specification 
           * and filter those customers that match the provided customer type
           */
          val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
          ds.map(x => ((x._1,x._2),(x._3,x._4))).join(parquetCST).map(x => {
            
            val ((site,user),((group,item),rfm_type)) = x
            /*
             * Note, that we replace the 'user' by the respective rfm_type
             */
            ParquetASR(site,rfm_type.toString,group,item)
            
          })
        })               
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/ASR-%s/%s""",requestCtx.getBase,customer.toString,uid)         
        table.saveAsParquetFile(store)

        requestCtx.listener ! String.format("""[INFO][UID: %s] ASR preparation for customer type '%s' finished.""",uid,customer.toString)

        val params = Map(Names.REQ_MODEL -> "ASR") ++ req_params
        context.parent ! PrepareFinished(params)
        
      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] ASR preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
}