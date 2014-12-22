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

import org.apache.spark.SparkContext
import akka.actor.{ActorRef,Props}

import de.kp.spark.core.Names

import de.kp.shopify.insight.RemoteContext
import de.kp.shopify.insight.model._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.HashMap

class Pipeliner(@transient sc:SparkContext,listener:ActorRef) extends BaseActor {
  /*
   * Reference to the remote Akka context to interact with
   * the Association Analysis and also the Intent Recognition
   * engine of Predictiveworks
   */
  private val rtx = new RemoteContext()
  override def receive = {
    
    /*
     * The data pipeline starts with a 'collection' of the Shopify orders
     * of a certain time period; the default is 30 days back from now;
     * 
     * the Pipeliner actor appends additional request parameters for the
     * Shopify REST API to restrict the orders, to paid and closed orders
     * from the last 30, 60 or 90 days 
     */
    case message:StartPipeline => {
      
      try {      
        
        val params = message.data
      
        val uid = params(Names.REQ_UID)      
        val sink = params(Names.REQ_SINK)
        /*
         * The Pipeline actor is responsible for creating an appropriate
         * actor that executes the collection request
         */
        sink match {
        
          case Sinks.ELASTIC => {
            /*
             * Send request to ElasticFeeder actor and inform requestor
             * that the tracking process has been started. Error and
             * interim messages of this process are sent to the listener
             */
            val actor = context.actorOf(Props(new ElasticCollector(listener)))          
            val new_params = params ++ addParams(params)
            
            actor ! StartCollect(new_params)
          
          }
        
          case _ => throw new Exception(String.format("""[UID: %s] The sink '%s' is not supported.""",uid,sink))
      
        }
    
      } catch {
        /*
         * Inform the message listener about the error that occurred
         * while collecting data from a certain Shopify store
         */
        case e:Exception => listener ! e.getMessage

      } 
      
    }
    
    case message:CollectFinished => {
      /*
       * This message is sent by a collector actor and indicates that the data collection
       * sub process has been finished. Note, that this collector (child) is responsible 
       * for stopping itself, and NOT the Pipeliner.
       * 
       * After having received this message, the Pipeliner actor starts to build the models;
       * to this end, actually three different models have to built by invoking the Association
       * Analysis and Intent Recognition engine of Predictiveworks.
       */
      
      /*
       * The ASRBuilder is responsible for building an association rule model
       * from the data registered in the 'items' index
       */
      val asr_builder = context.actorOf(Props(new ASRBuilder(rtx,listener)))  
      asr_builder ! StartBuild(message.data)

      /*
       * The STMBuilder is responsible for building a state transition model
       * from the data registered in the 'states' index
       */
      val stm_builder = context.actorOf(Props(new STMBuilder(rtx,listener)))  
      stm_builder ! StartBuild(message.data)
      
      /*
       * The HSMBuilder is responsible for building a hidden state model
       * from the data registered in the 'states' index
       */
      val hsm_builder = context.actorOf(Props(new HSMBuilder(rtx,listener)))  
      hsm_builder ! StartBuild(message.data)
      
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
         * The PRelaMBuilder is responsible for building a product rule model and
         * registering the result in an Elasticsearch index
         * 
         * ASR -> PRelaM
         */
        val prelam_builder = context.actorOf(Props(new PRelaMBuilder(rtx,listener)))  
        prelam_builder ! StartEnrich(message.data)
        /*
         * The PRecoMBuilder is responsible for building a product recommendation
         *  model and registering the result in an Elasticsearch index
         * 
         * ASR -> PRecoM
         */
        val precom_builder = context.actorOf(Props(new PRecoMBuilder(rtx,listener)))  
        precom_builder ! StartEnrich(message.data)
        
      } else if (model == "STM") {
        /*
         * The FCMBuilder is responsible for building a purchase forecast model
         * and registering the result in an Elasticsearch index
         * 
         * STM -> FCM
         */
        val fcm_builder = context.actorOf(Props(new FCMBuilder(rtx,listener)))  
        fcm_builder ! StartEnrich(message.data)
        
      } else if (model == "HSM") {
        
        // TODO
        
      } else {
        
        /* 
         * Do nothing as the model description is implemented and cannot be
         * manipulated by external requests: A model other than the ones
         * specified cannot appear
         */
        
      }
      
      
    }
    
    case message:EnrichFinished => {
      
      // TODO
      
    }
    
    case _ => {/* do nothing */}
    
  }
  
  private def addParams(params:Map[String,String]):Map[String,String] = {

    val days = if (params.contains(Names.REQ_DAYS)) params(Names.REQ_DAYS).toInt else 30
    
    val created_max = new DateTime()
    val created_min = created_max.minusDays(days)

    val data = HashMap(
      "created_at_min" -> formatted(created_min.getMillis),
      "created_at_max" -> formatted(created_max.getMillis)
    )
    
    /*
     * We restrict to those orders that have been paid,
     * and that are closed already, as this is the basis
     * for adequate forecasts 
     */
    data += "financial_status" -> "paid"
    data += "status" -> "closed"

    data.toMap
    
  }
  /**
   * This method is used to format a certain timestamp, provided with 
   * a request to collect data from a certain Shopify store
   */
  private def formatted(time:Long):String = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
    
    formatter.print(time)
    
  }

}