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
import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.actor.BaseActor

class ITPPreparer(requestCtx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor {
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      val uid = req_params(Names.REQ_UID)
      
      try {

        val sc = requestCtx.sparkContext
        
        val table = orders.groupBy(x => (x.site,x.user)).flatMap(x => {
          
          val (site,user) = x._1          
          val total = x._2.size
          
          val s0 = x._2.flatMap(_.items.map(v => (v.item,v.quantity)))
          val s1 = s0.groupBy(_._1)
          
          val supp = s1.map(v => (v._1, v._2.map(_._2).sum))      
          val pref = supp.map(v => (v._1,Math.log(1 + v._2.toDouble / total.toDouble)))
          
          supp.map(x => ParquetITP(site,user,x._1,x._2,pref(x._1),total))
          
          
        })
        
        val sqlCtx = new SQLContext(sc)
        import sqlCtx.createSchemaRDD

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
}