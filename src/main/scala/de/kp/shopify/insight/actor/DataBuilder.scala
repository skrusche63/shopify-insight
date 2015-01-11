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
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import akka.actor.Props
import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.actor.build._

import de.kp.shopify.insight.model._

import scala.collection.mutable.ArrayBuffer
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}
/**
 * The DataBuilder is distinguished by the 8 different customer types
 * defined in the context of this insight server.
 */
class DataBuilder(requestCtx:RequestContext) extends BaseActor(requestCtx) {
  
  private val STEPS = ArrayBuffer.empty[String]
  private val STEPS_COMPLETE = 3

  override def receive = {

    case message:StartBuild => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      requestCtx.listener ! String.format("""[INFO][UID: %s] Data preparation request received at %s.""",uid,start)

      /**********************************************************************
       *      
       *                       SUB PROCESS 'BUILD'
       * 
       *********************************************************************/
      
      createElasticIndexes(req_params)
      /*
       * Register this model building task in the respective
       * 'database/tasks' index
       */
      registerTask(req_params)
     
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
       * The Build actors (ASR,STM and HSM) already sent an error message 
       * to the message listener; what is left here is to forward the failed 
       * message to the data pipeline (parent)
       */
      context.parent ! BuildFailed(message.data)
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
      if (List("ASR","HSM","STM").contains(model)) STEPS += model
      
      if (STEPS.size == STEPS_COMPLETE) {

        val params = message.data.filter(kv => kv._1 == Names.REQ_MODEL)
        context.parent ! BuildFinished(params)
      
        context.stop(self)
        
      } else {
        
        /* 
         * Do nothing as thesub process is still going on
         */
        
      }
      
    }
    
    case _ => {/* do nothing */}
    
  }
  /**
   * This method registers the model building task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerTask(params:Map[String,String]) = {
    
    val uid = params(Names.REQ_UID)
    val key = "build:" + uid
    
    val task = "model building"
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

  private def createElasticIndexes(params:Map[String,String]) {
    
    val uid = params(Names.REQ_UID)
    /*
     * Create search indexes (if not already present)
     * 
     * The 'tasks' index (mapping) specified an administrative database
     * where all steps of a certain synchronization or data analytics
     * task are registered
     */
    
    if (requestCtx.createIndex(params,"database","tasks","task") == false)
      throw new Exception("Index creation for 'database/tasks' has been stopped due to an internal error.")
   
    requestCtx.listener ! String.format("""[INFO][UID: %s] Elasticsearch database/tasks index created.""",uid)
    
  }

}