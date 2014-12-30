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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._

import de.kp.shopify.insight.elastic._

import scala.collection.JavaConversions._

/**
 * RelationModeler is an actor that uses an association rule model, transforms
 * the model into a product rule model (PRM) and and registers the result in an
 * ElasticSearch index.
 * 
 * This is part of the 'enrich' sub process that represents the third component of 
 * the data analytics pipeline.
 * 
 */
class RelationModeler(requestCtx:RequestContext) extends BaseActor {

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

              val rules = buildProductRules(res)

              val handler = new ElasticClient()
              if (handler.putSources("orders","rules",rules) == false)
                throw new Exception("Indexing processing has been stopped due to an internal error.")

              requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PRELAM")            
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
  
  private def buildProductRules(response:ServiceResponse):List[java.util.Map[String,Object]] = {
            
    val uid = response.data(Names.REQ_UID)
    val rules = Serializer.deserializeRules(response.data(Names.REQ_RESPONSE))
    
    /*
     * Determine timestamp for the actual set of rules to be indexed
     */
    val now = new java.util.Date()
    val timestamp = now.getTime()
    
    rules.items.map(rule => {
      
      val source = new java.util.HashMap[String,Object]()    
      
      source += Names.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
      source += Names.UID_FIELD -> uid
      
      source += Names.ANTECEDENT_FIELD -> rule.antecedent
      source += Names.CONSEQUENT_FIELD -> rule.consequent
        
      source += Names.SUPPORT_FIELD -> rule.support.asInstanceOf[Object]
      source += Names.CONFIDENCE_FIELD -> rule.confidence.asInstanceOf[Object]

      source += Names.TOTAL_FIELD -> rule.total.asInstanceOf[Object]
        
      source
      
    })
    
  }
  
}