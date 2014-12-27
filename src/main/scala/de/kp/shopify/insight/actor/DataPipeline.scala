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

import de.kp.shopify.insight.{FindContext,PrepareContext}

import de.kp.shopify.insight.actor.build._
import de.kp.shopify.insight.actor.enrich._
import de.kp.shopify.insight.actor.profile._

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.model._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.{ArrayBuffer,HashMap}

class DataPipeline(prepareContext:PrepareContext,findContext:FindContext) extends BaseActor {
  
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
        prepareContext.clear
        
        val req_params = message.data
      
        val uid = req_params(Names.REQ_UID)      
        val sink = req_params(Names.REQ_SINK)
        /*
         * The Pipeline actor is responsible for creating an appropriate
         * actor that executes the collection request
         */
        sink match {
        
          case Sinks.ELASTIC => {
            
            createElasticIndexes(req_params)
            /*
             * Send request to ElasticCollector actor and inform requestor
             * that the tracking process has been started. Error and
             * interim messages of this process are sent to the listener
             */
            val actor = context.actorOf(Props(new ElasticCollector(prepareContext)))          
            actor ! StartCollect(req_params)
          
          }
        
          case _ => throw new Exception(String.format("""[ERROR][UID: %s] The sink '%s' is not supported.""",uid,sink))
      
        }
    
      } catch {
        
        case e:Exception => {
          /*
           * Inform the message listener about the error that occurred
           * while collecting data from a certain Shopify store and
           * stop the DataPipeline
           */
          prepareContext.listener ! e.getMessage
          
          prepareContext.clear
          context.stop(self)
          
        }

      } 
      
    }   
    case message:CollectFailed => {
      /*
       * The Collector actor already sent an error message to the message listener;
       * no additional notification has to be done, so just stop the pipeline
       */
      prepareContext.clear
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
      val asr_builder = context.actorOf(Props(new ASRBuilder(prepareContext)))  
      asr_builder ! StartBuild(message.data)

      /*
       * The STMBuilder is responsible for building a state transition model
       * from the data registered in the 'states' index
       */
      val stm_builder = context.actorOf(Props(new STMBuilder(prepareContext)))  
      stm_builder ! StartBuild(message.data)
      
      /*
       * The HSMBuilder is responsible for building a hidden state model
       * from the data registered in the 'states' index
       */
      val hsm_builder = context.actorOf(Props(new HSMBuilder(prepareContext)))  
      hsm_builder ! StartBuild(message.data)
      
    }    
    case message:BuildFailed => {
      /*
       * The Builder actors (ASR,STM and HSM) already sent an error message to the message 
       * listener; no additional notification has to be done, so just stop the pipeline
       */
      prepareContext.clear
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
        val prelam_modeler = context.actorOf(Props(new RelationModeler(prepareContext)))  
        prelam_modeler ! StartEnrich(message.data)
        /*
         * The RecommendationModeler is responsible for building a product recommendation
         *  model and registering the result in an Elasticsearch index
         * 
         * ASR -> URECOM
         */
        val urecom_modeler = context.actorOf(Props(new RecommendationModeler(prepareContext)))  
        urecom_modeler ! StartEnrich(message.data)
        
      } else if (model == "STM") {
        /*
         * The ForecastModeler is responsible for building a purchase forecast model
         * and registering the result in an Elasticsearch index
         * 
         * STM -> UFORCM
         */
        val uforcm_modeler = context.actorOf(Props(new ForecastModeler(prepareContext)))  
        uforcm_modeler ! StartEnrich(message.data)
        
      } else if (model == "HSM") {
        /*
         * The LoyaltyModeler is responsible for building a user loyalty model
         * and registering the result in an Elasticsearch index
         * 
         * HSM -> ULOYAM
         */
        val uloya_modeler = context.actorOf(Props(new LoyaltyModeler(prepareContext)))  
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
      prepareContext.clear
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
         
        val user_profiler = context.actorOf(Props(new UserProfiler(prepareContext,findContext)))  
        user_profiler ! StartProfile(message.data)
         
        val product_profiler = context.actorOf(Props(new ProductProfiler(prepareContext)))  
        product_profiler ! StartProfile(message.data)
        
      }
      
    }
    case message:ProfileFailed => {
      
    }
    case message:ProfileFinished => {
      
    }
    case _ => {/* do nothing */}
    
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
     * The 'recommendation' index (mapping) specifies a product recommendation
     * database derived from the Association rules and the last items purchased
     * 
     * The 'rule' index (mapping) specifies the association rules database
     * computed by the Association Analysis engine
     */
    val handler = new ElasticHandler()
    /*
     * SUB PROCESS 'COLLECT'
     */
    if (handler.createIndex(params,"orders","items","item") == false)
      throw new Exception("Index creation for 'orders/items' has been stopped due to an internal error.")
 
    if (handler.createIndex(params,"orders","states","state") == false)
      throw new Exception("Index creation for 'orders/states' has been stopped due to an internal error.")
    /*       
     * SUB PROCESS 'ENRICH'
     */
    if (handler.createIndex(params,"orders","forecasts","forecast") == false)
      throw new Exception("Indexing has been stopped due to an internal error.")

    if (handler.createIndex(params,"orders","loyalty","loyalty") == false)
      throw new Exception("Indexing has been stopped due to an internal error.")
            
    if (handler.createIndex(params,"orders","recommendations","recommendation") == false)
      throw new Exception("Indexing has been stopped due to an internal error.")
            
    if (handler.createIndex(params,"orders","rules","rule") == false)
      throw new Exception("Indexing has been stopped due to an internal error.")
    /*       
     * SUB PROCESS 'PROFILE'
     */
  
    prepareContext.listener ! String.format("""[INFO][UID: %s] Elasticsearch indexes created.""",uid)
    
  }
  
}