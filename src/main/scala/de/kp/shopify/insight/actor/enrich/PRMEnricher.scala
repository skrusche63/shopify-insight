package de.kp.shopify.insight.actor.enrich
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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.elastic._

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * PRMEnricher is an actor that uses an association rule model, transforms
 * the model into a product rule model (PRM) and registers the result as a
 * Parquet file.
 */
class PRMEnricher(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
        
        requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model building started.""",uid)
        
        val (service,request) = buildRemoteRequest(req_params)

        val response = requestCtx.getRemoteContext.send(service,request).mapTo[String]            
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {

              val sc = requestCtx.sparkContext
              val table = buildTable(res)
        
              val sqlCtx = new SQLContext(sc)
              import sqlCtx.createSchemaRDD

              /* 
               * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
               * allowing it to be stored using Parquet. 
               */
              val store = String.format("""%s/PRM/%s""",requestCtx.getBase,uid)         
              table.saveAsParquetFile(store)

              requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PRM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
            
            }
          
          }
          
        }
        /*
         * The Association Analysis engine returned an error message
         */
        response.onFailure {
          case throwable => {
                    
            requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
         
      } catch {
        case e:Exception => {
                    
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }

  private def buildRemoteRequest(params:Map[String,String]):(String,String) = {

    val service = "association"
    val task = "get:rule"

    val data = new ASRHandler().get(params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  
  private def buildTable(response:ServiceResponse):RDD[ParquetPRM] = {
            
    val sc = requestCtx.sparkContext
    val rules = Serializer.deserializeRules(response.data(Names.REQ_RESPONSE))
   
    val relations = rules.items.map(rule => {
      ParquetPRM(rule.antecedent,rule.consequent,rule.support,rule.total,rule.confidence)	  
    })
    
    sc.parallelize(relations)
    
  }
  
}