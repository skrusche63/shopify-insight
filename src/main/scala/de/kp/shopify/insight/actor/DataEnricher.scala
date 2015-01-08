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

import de.kp.shopify.insight.actor.enrich._
import de.kp.shopify.insight.model._

import scala.collection.mutable.ArrayBuffer
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class DataEnricher(requestCtx:RequestContext) extends BaseActor {
  
  private val STEPS = ArrayBuffer.empty[String]
  private val STEPS_COMPLETE = 4

  override def receive = {

    case message:StartEnrich => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      requestCtx.listener ! String.format("""[INFO][UID: %s] Data enrichment request received at %s.""",uid,start)

      /**********************************************************************
       *      
       *                       SUB PROCESS 'ENRICH'
       * 
       *********************************************************************/
      
      createElasticIndexes(req_params)
      /*
       * Register this data enrichment task in the respective
       * 'database/tasks' index
       */
      registerTask(req_params)

      /*
       * The PRMEnricher starts from the prior ASR model and is responsible 
       * for building a product rule model, PRM, and storing the result as 
       * Parquet file
       */
      val prm_enricher = context.actorOf(Props(new PRMEnricher(requestCtx)))  
      prm_enricher ! StartEnrich(message.data)
      /*
       * The URMEnricher starts from the prior ASR model and is responsible 
       * for building a product recommendation model, URM, and storing the 
       * result as Parquet file
       */
      val urm_enricher = context.actorOf(Props(new URMEnricher(requestCtx)))  
      urm_enricher ! StartEnrich(message.data)
      /*
       * The UFMEnricher starts from the prior STM model and is responsible 
       * for building a purchase forecast model, UFM, and storing the result 
       * as Parquet file
       */
      val ufm_enricher = context.actorOf(Props(new UFMEnricher(requestCtx)))  
      ufm_enricher ! StartEnrich(message.data)
      /*
       * The ULMEnricher starts from the prior HSM model and is responsible 
       * for building a user loyalty model, ULM, and storing the result as 
       * Parquet file
       */
      val ulm_enricher = context.actorOf(Props(new ULMEnricher(requestCtx)))  
      ulm_enricher ! StartEnrich(message.data)
      
    }
    case message:EnrichFailed => {
      /*
       * The Enrich actors (PRELAM,UFM, ULM and URM) already sent an error 
       * message to the message listener; what is left here is to forward 
       * the failed message to the data pipeline (parent)
       */
      context.parent ! EnrichFailed(message.data)
      context.stop(self)

    }
    case message:EnrichFinished => {
      
      val model = message.data(Names.REQ_MODEL)
      if (List("PRM","URM","UFM","ULM").contains(model)) STEPS += model
      
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
   * This method registers the data preparation task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerTask(params:Map[String,String]) = {
    
    val uid = params(Names.REQ_UID)
    val key = "enrich:" + uid
    
    val task = "data enrichment"
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