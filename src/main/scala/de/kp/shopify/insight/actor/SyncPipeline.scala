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

import de.kp.shopify.insight.PrepareContext

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.elastic._

import de.kp.shopify.insight.actor.synchronize._

class SyncPipeline(prepareContext:PrepareContext) extends BaseActor {

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
       
        val customer_sync = context.actorOf(Props(new CustomerSync(prepareContext)))  
        customer_sync ! StartSynchronize(message.data)
      
        val product_sync = context.actorOf(Props(new ProductSync(prepareContext)))  
        product_sync ! StartSynchronize(message.data)
        
    
      } catch {
        
        case e:Exception => {
          /*
           * Inform the message listener about the error that occurred while collecting 
           * data from a certain Shopify store and stop the synchronization pipeline
           */
          prepareContext.listener ! e.getMessage
          
          prepareContext.clear
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
      // TODO
      context.stop(self)

    }
    
  }
  
  /**
   * A helper method to prepare all Elasticsearch indexes used by the 
   * Shopify Analytics (or Insight) Server
   */
  private def createElasticIndexes(params:Map[String,String]) {
    
    val uid = params(Names.REQ_UID)
    
    val handler = new ElasticHandler()
    /*
     * SUB PROCESS 'SYNCHRONIZE'
     */
    if (handler.createIndex(params,"database","customers","customer") == false)
      throw new Exception("Index creation for 'database/customers' has been stopped due to an internal error.")
 
    if (handler.createIndex(params,"database","products","product") == false)
      throw new Exception("Index creation for 'database/products' has been stopped due to an internal error.")
    
  }
  
}