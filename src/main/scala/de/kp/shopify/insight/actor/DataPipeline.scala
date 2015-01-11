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

class DataPipeline(requestCtx:RequestContext) extends BaseActor(requestCtx) {

  override def receive = {
    
    case message:StartPipeline => {
      
      /**********************************************************************
       *      
       *                       SUB PROCESS 'COLLECT'
       * 
       *********************************************************************/
       
      val req_params = message.data
      /*
       * Send request to DataCollector actor and inform requestor
       * that the tracking process has been started. Error and
       * interim messages of this process are sent to the listener
       */
      val actor = context.actorOf(Props(new DataCollector(requestCtx)))          
      actor ! StartSynchronize(req_params)
      
    }       
    case message:CollectFailed => {
      /*
       * The DataSynchronizer actor already sent an error message to the message 
       * listener; no additional notification has to be done, so just stop the 
       * pipeline
       */
      context.stop(self)
       
    }
    case message:SyncCollectshed => {
     /*
       * This message is sent by the DataSynchronizer actor and indicates that the 
       * data synchronization sub process has been finished. Note, that this actor 
       * (child) is responsible for stopping itself, and NOT the DataPipeline.
       * 
       * After having received this message, the DataPipeline actor starts to prepare 
       * the data; to this end, the DataPreparer actor is invoked.
       */

      /**********************************************************************
       *      
       *                       SUB PROCESS 'PREPARE'
       * 
       *********************************************************************/
        
      val req_params = message.data
      /*
       * Send request to DataPreparer actor and inform requestor
       * that the tracking process has been started. Error and
       * interim messages of this process are sent to the listener
       */
      val actor = context.actorOf(Props(new DataPreparer(requestCtx)))          
      actor ! StartPrepare(req_params)
      
    }
    case message:PrepareFailed => {
      /*
       * The DataPreparer actor already sent an error message to the message listener;
       * no additional notification has to be done, so just stop the pipeline
       */
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
      
      // TODO
      val customerType = 1
      
      val actor = context.actorOf(Props(new DataBuilder(requestCtx,customerType)))  
      actor ! StartBuild(message.data)
      
    }    
    case message:BuildFailed => {
      /*
       * The DataBuilder actors (ASR,STM and HSM) already sent an error message to 
       * the message listener; no additional notification has to be done, so just 
       * stop the pipeline
       */
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
    
    case message:LoadFailed => {
      // TODO
    }
    
    case message:LoadFinished => {
      
    }
    case _ => {/* do nothing */}
    
  }
  
}