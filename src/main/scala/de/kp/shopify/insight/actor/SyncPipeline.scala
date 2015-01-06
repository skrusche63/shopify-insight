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

import akka.actor.Props

import de.kp.spark.core.Names

import de.kp.shopify.insight._

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.elastic._

import de.kp.shopify.insight.actor.synchronize._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.mutable.ArrayBuffer

class SyncPipeline(requestCtx:RequestContext) extends BaseActor {
  
  private val MODELS = ArrayBuffer.empty[String]
  private val MODELS_COMPLETE = 3

  override def receive = {
    
    case message:StartPipeline => {
      
      /**********************************************************************
       *      
       *                       SUB PROCESS 'SYNCHRONIZE'
       * 
       *********************************************************************/
      try { 
        
        val req_params = message.data
        createElasticIndexes(req_params)
       
        /*
         * Register this synchronization task in the respective
         * 'database/tasks' index
         */
        registerTask(req_params)
        
        /*
         * Synchronize customer and product database with the 
         * current entries assigned to s specific Shopify store
         */
        val customer_sync = context.actorOf(Props(new CustomerSync(requestCtx)))  
        customer_sync ! StartSynchronize(message.data)
      
        val product_sync = context.actorOf(Props(new ProductSync(requestCtx)))  
        product_sync ! StartSynchronize(message.data)
        
    
      } catch {
        
        case e:Exception => {
          /*
           * Inform the message listener about the error that occurred while collecting 
           * data from a certain Shopify store and stop the synchronization pipeline
           */
          requestCtx.listener ! e.getMessage
          
          requestCtx.clear
          context.stop(self)
          
        }

      } 
      
    }
    case message:SynchronizeFailed => {
      /*
       * The synchronizer actors already sent an error message to the message listener;
       * no additional notification has to be done, so just stop the pipeline
       */
      context.stop(self)
      
    }
    case message:SynchronizeFinished => {
      /*
       * Collect the models built by the synchronization sub processes,
       * and if synchronization task is finished, stop pipeline actor
       */
      val model = message.data(Names.REQ_MODEL)
      if (List("CUSTOMER","PRODUCT","ORDER").contains(model)) MODELS += model
      
      if (MODELS.size == MODELS_COMPLETE) context.stop(self)

    }
    
  }
  /**
   * This method registers the synchronization task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerTask(params:Map[String,String]) = {
    
    val uid = params(Names.REQ_UID)
    val key = "synchronize:" + uid
    
    val task = "database synchronization"
    /*
     * Note, that we do not specify additional
     * payload data here
     */
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* key */
	builder.field("key",key)
	
	/* task */
	builder.field("task",task)
	
	/* timestamp */
	builder.field("timestamp",params("timestamp").toLong)

    /* created_at_min */
	builder.field("created_at_min",params("created_at_min"))
	
    /* created_at_max */
	builder.field("created_at_max",params("created_at_max"))
	
	builder.endObject()
	/*
	 * Register data in the 'database/tasks' index
	 */
	requestCtx.putSource("database","tasks",builder)

  }
  
  /**
   * A helper method to prepare all Elasticsearch indexes used by the 
   * Shopify Analytics (or Insight) Server
   */
  private def createElasticIndexes(params:Map[String,String]) {
    
    val uid = params(Names.REQ_UID)
    /*
     * Create search indexes (if not already present)
     * 
     * The 'tasks' index (mapping) specified an administrative database
     * where all steps of a certain synchronization or data analytics
     * task are registered
     * 
     * The 'customers' index (mapping) specifies a customer database that
     * holds synchronized customer data relevant for the insight server
     * 
     * The 'products' index (mapping) specifies a product database that
     * holds synchronized product data relevant for the insight server
     * 
     * The 'orders' index (mapping) specifies an order database that
     * holds synchronized order data relevant for the insight server
     */
    
    if (requestCtx.createIndex(params,"database","tasks","task") == false)
      throw new Exception("Index creation for 'database/tasks' has been stopped due to an internal error.")
    
    /*
     * SUB PROCESS 'SYNCHRONIZE'
     */
    if (requestCtx.createIndex(params,"database","customers","customer") == false)
      throw new Exception("Index creation for 'database/customers' has been stopped due to an internal error.")
 
    if (requestCtx.createIndex(params,"database","products","product") == false)
      throw new Exception("Index creation for 'database/products' has been stopped due to an internal error.")
 
    if (requestCtx.createIndex(params,"database","orders","order") == false)
      throw new Exception("Index creation for 'database/orders' has been stopped due to an internal error.")
    
  }
  
}