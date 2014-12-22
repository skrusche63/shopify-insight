package de.kp.shopify.insight.actor
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

import de.kp.shopify.insight.{ServerContext,ShopifyContext}
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.elastic._

import de.kp.shopify.insight.source._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.collection.JavaConversions._


/**
 * PRelaMBuilder is an actor that uses an association rule model, transforms
 * the model into a product rule model (PRM) and and registers the result 
 * in an ElasticSearch index.
 * 
 * This is part of the 'enrich' sub process that represents the third component of 
 * the data analytics pipeline.
 * 
 */
class PRelaMBuilder(serverContext:ServerContext) extends BaseActor {

  private val stx = new ShopifyContext(serverContext.listener)  

  override def receive = {
   
    case message:StartEnrich => {
      
      try {

        val params = message.data
        val uid = params(Names.REQ_UID)
        
        /*
         * STEP #1: Retrieve association rules from the Association 
         * Analysis engine
         */      
        val (service,request) = buildRemoteRequest(params)

        val response = serverContext.getRemoteContext.send(service,request).mapTo[String]            
        response.onSuccess {
        
          case result => {
 
            val intermediate = Serializer.deserializeResponse(result)
            val rules = buildProductRules(intermediate)
            /*
             * STEP #2: Create search index (if not already present);
             * the index is used to register the rules derived from
             * the association rule model
             */
            val handler = new ElasticHandler()
            
            if (handler.createIndex(params,"orders","rules","rule") == false)
              throw new Exception("Indexing has been stopped due to an internal error.")
 
            serverContext.listener ! String.format("""[UID: %s] Elasticsearch index created.""",uid)

            if (handler.putRules("orders","rules",rules) == false)
              throw new Exception("Indexing processing has been stopped due to an internal error.")

            serverContext.listener ! String.format("""[UID: %s] Product rule perspective registered in Elasticsearch index.""",uid)

            val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PRelaM")            
            context.parent ! EnrichFinished(data)           
            
            context.stop(self)
             
          }
          
        }
        /*
         * The Association Analysis engine returned an error message
         */
        response.onFailure {
          case throwable => {
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
         
      } catch {
        case e:Exception => {
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }
  /**
   * The Cross-Sell Model (CSM) builder combines the association rules retrieved
   * from the Association Analysis engine with products from a Shopify store
   */
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
   
    rules.items.flatMap(rule => {
      
      /* 
       * Unique identifier to group all entries that refer to the same rule
       */      
      val rid = java.util.UUID.randomUUID().toString()
      /*
       * The rule antecedents are indexed as single documents
       * with an additional weight, derived from the total number
       * of antecedents per rule
       */
      rule.antecedent.map(item => {
      
        val source = new java.util.HashMap[String,Object]()    
      
        source += Names.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
        source += Names.UID_FIELD -> uid
      
        source += Names.RULE_FIELD -> rid
        
        source += Names.ANTECEDENT_FIELD -> item.asInstanceOf[Object]
        source += Names.CONSEQUENT_FIELD -> rule.consequent
        
        source += Names.SUPPORT_FIELD -> rule.support.asInstanceOf[Object]
        source += Names.CONFIDENCE_FIELD -> rule.confidence.asInstanceOf[Object]
        
        source += Names.WEIGHT_FIELD -> (1.toDouble / rule.antecedent.length).asInstanceOf[Object]
        
        source
      
      })
      
    })
    
  }
  
}