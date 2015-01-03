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

import de.kp.shopify.insight.actor.build._
import de.kp.shopify.insight.actor.enrich._
import de.kp.shopify.insight.actor.profile._

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.model._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.{ArrayBuffer,HashMap}
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class DataPipeline(requestCtx:RequestContext) extends BaseActor {
  
  private val MODELS = ArrayBuffer.empty[String]
  private val MODELS_COMPLETE = 4
  /*
   * Reference to the remote Akka context to interact with
   * the Association Analysis and also the Intent Recognition
   * engine of Predictiveworks
   */
  override def receive = {
    
    /*
     * The data pipeline starts with a 'collection' of the Shopify orders
     * of a certain time period; the default is 30 days back from now;
     * 
     * the DataPipeline actor appends additional request parameters for the
     * Shopify REST API to restrict the orders, to paid and closed orders
     * from the last 30, 60 or 90 days 
     */
    case message:StartPipeline => {
      
      /**********************************************************************
       *      
       *                       SUB PROCESS 'COLLECT'
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
         * Send request to DataCollector actor and inform requestor
         * that the tracking process has been started. Error and
         * interim messages of this process are sent to the listener
         */
        val actor = context.actorOf(Props(new DataCollector(requestCtx)))          
        actor ! StartCollect(req_params)
    
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
    case message:CollectFailed => {
      /*
       * The Collector actor already sent an error message to the message listener;
       * no additional notification has to be done, so just stop the pipeline
       */
      requestCtx.clear
      context.stop(self)
      
    }    
    case message:CollectFinished => {
      /*
       * This message is sent by a collector actor and indicates that the data collection
       * sub process has been finished. Note, that this collector (child) is responsible 
       * for stopping itself, and NOT the DataPipeline.
       * 
       * After having received this message, the DataPipeline actor starts to build the models;
       * to this end, actually three different models have to built by invoking the Association
       * Analysis and Intent Recognition engine of Predictiveworks.
       */

      /**********************************************************************
       *      
       *                       SUB PROCESS 'ENRICH'
       * 
       *********************************************************************/
      
      /*
       * The ASRBuilder is responsible for building an association rule model
       * from the data registered in the 'items' index
       */
      val asr_builder = context.actorOf(Props(new ASRBuilder(requestCtx)))  
      asr_builder ! StartBuild(message.data)

      /*
       * The STMBuilder is responsible for building a state transition model
       * from the data registered in the 'states' index
       */
      val stm_builder = context.actorOf(Props(new STMBuilder(requestCtx)))  
      stm_builder ! StartBuild(message.data)
      
      /*
       * The HSMBuilder is responsible for building a hidden state model
       * from the data registered in the 'states' index
       */
      val hsm_builder = context.actorOf(Props(new HSMBuilder(requestCtx)))  
      hsm_builder ! StartBuild(message.data)
      
    }    
    case message:BuildFailed => {
      /*
       * The Builder actors (ASR,STM and HSM) already sent an error message to the message 
       * listener; no additional notification has to be done, so just stop the pipeline
       */
      requestCtx.clear
      context.stop(self)
      
    }    
    case message:BuildFinished => {
      /*
       * This message is sent by one of the Builder actors and indicates that the building
       * of a certain model has been successfully finished. We distinguish the following
       * model types:
       * 
       * ASR: Specifies the association rule model that is the basis for 'collection',
       * 'cross-sell' and 'promotion' requests. 
       * 
       * STM: Specifies the state transition model that is the basis for purchase 
       * forecasts, that have to be built from this model.
       * 
       * HSM: Specifies a hidden state model that is the basis for loyalty state
       * forecasts, that have to be built from this model
       */  
      val model = message.data(Names.REQ_MODEL)
      if (model == "ASR") {
        /*
         * The RelationModeler is responsible for building a product rule model and
         * registering the result in an Elasticsearch index
         * 
         * ASR -> PRELAM
         */
        val prelam_modeler = context.actorOf(Props(new RelationModeler(requestCtx)))  
        prelam_modeler ! StartEnrich(message.data)
        /*
         * The RecommendationModeler is responsible for building a product recommendation
         *  model and registering the result in an Elasticsearch index
         * 
         * ASR -> URECOM
         */
        val urecom_modeler = context.actorOf(Props(new RecommendationModeler(requestCtx)))  
        urecom_modeler ! StartEnrich(message.data)
        
      } else if (model == "STM") {
        /*
         * The ForecastModeler is responsible for building a purchase forecast model
         * and registering the result in an Elasticsearch index
         * 
         * STM -> UFORCM
         */
        val uforcm_modeler = context.actorOf(Props(new ForecastModeler(requestCtx)))  
        uforcm_modeler ! StartEnrich(message.data)
        
      } else if (model == "HSM") {
        /*
         * The LoyaltyModeler is responsible for building a user loyalty model
         * and registering the result in an Elasticsearch index
         * 
         * HSM -> ULOYAM
         */
        val uloya_modeler = context.actorOf(Props(new LoyaltyModeler(requestCtx)))  
        uloya_modeler ! StartEnrich(message.data)
        
      } else {
        
        /* 
         * Do nothing as the model description is implemented and cannot be
         * manipulated by external requests: A model other than the ones
         * specified cannot appear
         */
        
      }
      
    }
    case message:EnrichFailed => {
      /*
       * The Enrich actors (URECOM,PRELAM,UFORCM and ULOYAM) already sent an error message 
       * to the message listener; no additional notification has to be done, so just stop 
       * the pipeline
       */
      requestCtx.clear
      context.stop(self)

    }
    case message:EnrichFinished => {
      /*
       * Collect the models built by the enrichment sub processes
       */
      val model = message.data(Names.REQ_MODEL)
      if (List("PRELAM","UFORCM","ULOYAM","URECOM").contains(model)) MODELS += model
      
      if (MODELS.size == MODELS_COMPLETE) {
        /*
         * The final step within the data analytics pipeline
         * is the generation of product and user profiles
         */
      
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
     * The 'aggregates' index (mapping) specifies a database with aggregated
     * data from all orders or transactions of a certain time span
     * 
     * The 'items' index (mapping) specifies a transaction database and
     * is used by Association Analysis, Series Analysis and other engines
     * 
     * The 'states' index (mapping) specifies a states database derived 
     * from the amount representation and used by Intent Recognition
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
     * SUB PROCESS 'COLLECT'
     */
    if (requestCtx.createIndex(params,"orders","aggregates","aggregate") == false)
      throw new Exception("Index creation for 'orders/aggregates' has been stopped due to an internal error.")

    if (requestCtx.createIndex(params,"users","items","item") == false)
      throw new Exception("Index creation for 'users/items' has been stopped due to an internal error.")
 
    if (requestCtx.createIndex(params,"users","states","state") == false)
      throw new Exception("Index creation for 'users/states' has been stopped due to an internal error.")
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