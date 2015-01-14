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

import scala.collection.mutable.ArrayBuffer
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class DataLoader(requestCtx:RequestContext) extends BaseActor(requestCtx) {
  
  private val STEPS = ArrayBuffer.empty[String]
  private val STEPS_COMPLETE = 5

  override def receive = {

    case message:StartLoad => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
 
      val start = new java.util.Date().getTime.toString            
      requestCtx.listener ! String.format("""[INFO][UID: %s] Model loading request received at %s.""",uid,start)

      /**********************************************************************
       *      
       *                       SUB PROCESS 'LOAD'
       * 
       *********************************************************************/
      
      createElasticIndexes(req_params)      
      /*
       * Register this model loading task in the respective
       * 'database/tasks' index
       */
      registerTask(req_params)
      
//      /*
//       * The PRMLoader is responsible for loading the product relation
//       * model into the product/rules index
//       */
//      val prm_loader = context.actorOf(Props(new PRMLoader(requestCtx)))  
//      prm_loader ! StartLoad(message.data)
//
//      /*
//       * The UFMLoader is responsible for loading the user purchase forecast
//       * model into the users/forecasts index
//       */
//      val ufm_loader = context.actorOf(Props(new UFMLoader(requestCtx)))  
//      ufm_loader ! StartBuild(message.data)
//      
//      /*
//       * The ULMLoader is responsible for loading the user loyalty model
//       * into the users/loyalties index
//       */
//      val ulm_loader = context.actorOf(Props(new ULMLoader(requestCtx)))  
//      ulm_loader ! StartBuild(message.data)
//      
//      /*
//       * The URMLoader is responsible for loading the user recommendation
//       * model into the users/recommendations index
//       */
//      val urm_loader = context.actorOf(Props(new URMLoader(requestCtx)))  
//      urm_loader ! StartBuild(message.data)
//      
//      /*
//       * The UMPLoader is responsible for loading the user movement
//       * profile into the users/locations index
//       */
//      val ump_loader = context.actorOf(Props(new UMPLoader(requestCtx)))  
//      ump_loader ! StartBuild(message.data)
      
    }    
    case message:LoadFailed => {
      /*
       * The Load actors (PRM,UFM,ULM and URM) already sent an error message 
       * to the message listener; what is left here is to forward the failed 
       * message to the data pipeline (parent)
       */
      context.parent ! LoadFailed(message.data)
      context.stop(self)
      
    }    
    case message:LoadFinished => {
      /*
       * This message is sent by one of the Loader actors and indicates that 
       * the loading of a certain model has been successfully finished.
       */  
      val model = message.data(Names.REQ_MODEL)
      if (List("PRM","UFM","ULM","UMP","URM").contains(model)) STEPS += model
      
      if (STEPS.size == STEPS_COMPLETE) {

        val params = message.data.filter(kv => kv._1 == Names.REQ_MODEL)
        context.parent ! LoadFinished(params)
      
        context.stop(self)
        
      } else {
        
        /* 
         * Do nothing as the sub process is still going on
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
    val key = "load:" + uid
    
    val task = "model loading"
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
     * 
     * The 'forecast' index (mapping) specifies a sales forecast database
     * derived from the Markovian rules built by the Intent Recognition
     * engine
     * 
     * The 'location' index (mapping) specifies a user movement database
     * derived from IP addresses and timestamps provided by user orders
     * 
     * The 'loyalty' index (mapping) specifies a user loyalty database
     * derived from the Markovian hidden states built by the Intent Recognition
     * engine
     * 
     * The 'recommendation' index (mapping) specifies a product recommendation
     * database derived from the Association rules and the last items purchased
     * 
     * The 'rule' index (mapping) specifies a product association rule database
     * computed by the Association Analysis engine
     */
    
    if (requestCtx.createIndex(params,"database","tasks","task") == false)
      throw new Exception("Index creation for 'database/tasks' has been stopped due to an internal error.")
    
    if (requestCtx.createIndex(params,"users","forecasts","forecast") == false)
      throw new Exception("Index creation for 'users/forecasts' has been stopped due to an internal error.")

    if (requestCtx.createIndex(params,"users","locations","location") == false)
      throw new Exception("Index creation for 'users/locations' has been stopped due to an internal error.")

    if (requestCtx.createIndex(params,"users","loyalties","loyalty") == false)
      throw new Exception("Index creation for 'users/loyalties' has been stopped due to an internal error.")
            
    if (requestCtx.createIndex(params,"users","recommendations","recommendation") == false)
      throw new Exception("Index creation for 'users/recommendations' has been stopped due to an internal error.")
            
    if (requestCtx.createIndex(params,"products","rules","rule") == false)
      throw new Exception("Index creation for 'products/rules' has been stopped due to an internal error.")
  
    requestCtx.listener ! String.format("""[INFO][UID: %s] Elasticsearch indexes created.""",uid)
    
  }

}