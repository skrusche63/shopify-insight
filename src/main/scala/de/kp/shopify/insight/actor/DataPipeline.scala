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
import de.kp.shopify.insight.actor.profile._

import de.kp.shopify.insight.model._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class DataPipeline(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
    
    case message:StartPipeline => {
      
      /**********************************************************************
       *      
       *                       SUB PROCESS 'PREPARE'
       * 
       *********************************************************************/
      try {      
        
        /*
         * The data analytics pipeline retrieves Shopify orders; we therefore
         * have to make clear, that no previous orders are still available
         */
        requestCtx.clear
        
        val req_params = message.data
        createElasticIndexes(req_params)
       
        /*
         * Register this data preparation task in the respective
         * 'database/tasks' index
         */
        registerTask(req_params)
        /*
         * Send request to DataPreparer actor and inform requestor
         * that the tracking process has been started. Error and
         * interim messages of this process are sent to the listener
         */
        val actor = context.actorOf(Props(new DataPreparer(requestCtx)))          
        actor ! StartPrepare(req_params)
    
      } catch {
        
        case e:Exception => {
          /*
           * Inform the message listener about the error that occurred
           * while collecting data from a certain Shopify store and
           * stop the DataPipeline
           */
          requestCtx.listener ! e.getMessage
          
          requestCtx.clear
          context.stop(self)
          
        }

      } 
      
    }   
    case message:PrepareFailed => {
      /*
       * The Collector actor already sent an error message to the message listener;
       * no additional notification has to be done, so just stop the pipeline
       */
      requestCtx.clear
      context.stop(self)
      
    }    
    case message:PrepareFinished => {
      /*
       * This message is sent by a DataPreparer actor and indicates that the data 
       * preparation sub process has been finished. Note, that this actor (child) 
       * is responsible for stopping itself, and NOT the DataPipeline.
       * 
       * After having received this message, the DataPipeline actor starts to build 
       * the models; to this end, the DataBuilder actor is invoked.
       */

      /**********************************************************************
       *      
       *                       SUB PROCESS 'BUILD'
       * 
       *********************************************************************/
      
      val actor = context.actorOf(Props(new DataBuilder(requestCtx)))  
      actor ! StartBuild(message.data)
      
    }    
    case message:BuildFailed => {
      /*
       * The DataBuilder actors (ASR,STM and HSM) already sent an error message to 
       * the message listener; no additional notification has to be done, so just 
       * stop the pipeline
       */
      requestCtx.clear
      context.stop(self)
      
    }    
    case message:BuildFinished => {
      /*
       * This message is sent by the DataBuilder actor and indicates that the model
       * building sub process has been successfully finished. Note, that this actor 
       * (child) is responsible for stopping itself, and NOT the DataPipeline.
       * 
       * After having received this message, the DataPipeline actor starts to enrich 
       * the enrich the shop data by applying the respective models
       */  

      /**********************************************************************
       *      
       *                       SUB PROCESS 'ENRICH'
       * 
       *********************************************************************/
      
      val actor = context.actorOf(Props(new DataEnricher(requestCtx)))  
      actor ! StartEnrich(message.data)
      
    }
    case message:EnrichFailed => {
      /*
       * The Enrich actors (PRM,UFM,ULM and URM) already sent an error message to the 
       * message listener; no additional notification has to be done, so just stop the 
       * pipeline
       */
      requestCtx.clear
      context.stop(self)

    }
    case message:EnrichFinished => {
      
      /********************************************************************
       *      
       *                     SUB PROCESS 'PROFILE'
       * 
       *******************************************************************/
         
      val user_profiler = context.actorOf(Props(new UserProfiler(requestCtx)))
      user_profiler ! StartProfile(message.data)
         
      val product_profiler = context.actorOf(Props(new ProductProfiler(requestCtx)))  
      product_profiler ! StartProfile(message.data)
      
    }
    case message:ProfileFailed => {
      // TODO
    }
    case message:ProfileFinished => {
      // TODO
    }
    case _ => {/* do nothing */}
    
  }
  /**
   * This method registers the data preparation task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerTask(params:Map[String,String]) = {
    
    val uid = params(Names.REQ_UID)
    val key = "prepare:" + uid
    
    val task = "data preparation"
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
     * The 'forecast' index (mapping) specifies a sales forecast database
     * derived from the Markovian rules built by the Intent Recognition
     * engine
     * 
     * The 'loyalty' index (mapping) specifies a user loyalty database
     * derived from the Markovian hidden states built by the Intent Recognition
     * engine
     * 
     * The 'recommendation' index (mapping) specifies a product recommendation
     * database derived from the Association rules and the last items purchased
     * 
     * The 'rule' index (mapping) specifies the association rules database
     * computed by the Association Analysis engine
     * 
     * The 'profile' index (mapping) specifies the user profile database
     * computed by the user profiler
     */
    
    if (requestCtx.createIndex(params,"database","tasks","task") == false)
      throw new Exception("Index creation for 'database/tasks' has been stopped due to an internal error.")
    
    /*       
     * SUB PROCESS 'ENRICH'
     */
    if (requestCtx.createIndex(params,"users","forecasts","forecast") == false)
      throw new Exception("Index creation for 'users/forecasts' has been stopped due to an internal error.")

    if (requestCtx.createIndex(params,"users","loyalties","loyalties") == false)
      throw new Exception("Index creation for 'users/loyalties' has been stopped due to an internal error.")
            
    if (requestCtx.createIndex(params,"users","recommendations","recommendation") == false)
      throw new Exception("Index creation for 'users/recommendations' has been stopped due to an internal error.")
            
    if (requestCtx.createIndex(params,"products","rules","rule") == false)
      throw new Exception("Index creation for 'products/rules' has been stopped due to an internal error.")
    /*       
     * SUB PROCESS 'PROFILE'
     */           
    if (requestCtx.createIndex(params,"users","profiles","profile") == false)
      throw new Exception("Index creation for 'users/profiles' has been stopped due to an internal error.")
  
    requestCtx.listener ! String.format("""[INFO][UID: %s] Elasticsearch indexes created.""",uid)
    
  }
  
}