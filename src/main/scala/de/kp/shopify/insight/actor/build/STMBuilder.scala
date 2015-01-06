package de.kp.shopify.insight.actor.build
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

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

import de.kp.shopify.insight.actor.BaseActor

/**
 * STMBuilder is responsible for building a state transition model
 * by invoking the Intent Recognition engine of Predictiveworks.
 * 
 * This is part of the 'build' sub process that represents the second
 * component of the data analytics pipeline.
 * 
 */
class STMBuilder(requestCtx:RequestContext) extends BaseActor {
  
  private val config = requestCtx.getConfig
  
  override def receive = {
   
    case message:StartBuild => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      /* 
       * Build service request message to invoke remote Intent Recognition 
       * engine to train a state transition model from a 'states' index
       */
      val service = "intent"
      val task = "train"

      val data = new STMHandler().train(req_params)
      val req  = new ServiceRequest(service,task,data)
      
      val serialized = Serializer.serializeRequest(req)
      val response = requestCtx.getRemoteContext.send(service,serialized).mapTo[String]  
      
      requestCtx.listener ! String.format("""[INFO][UID: %s] State transition model building started.""",uid)

      /*
       * The RemoteSupervisor actor monitors the Redis cache entries of this
       * state transition model building request and informs this actor (as parent)
       * that a certain status has been reached
       */
      val status = ResponseStatus.MODEL_TRAINING_FINISHED
      val supervisor = context.actorOf(Props(new Supervisor(req,status,config)))
      
      /*
       * We evaluate the response message from the remote
       * Intent Recognition engine to check whether an
       * error occurred
       */
      response.onSuccess {
        
        case result => {
 
          val res = Serializer.deserializeResponse(result)
          if (res.status == ResponseStatus.FAILURE) {
      
            requestCtx.listener ! String.format("""[INFO][UID: %s] State transition model building failed due to an engine error.""",uid)
 
            context.parent ! BuildFailed(res.data)
            context.stop(self)

          }
         
        }

      }
      response.onFailure {
          
        case throwable => {
      
          requestCtx.listener ! String.format("""[INFO][UID: %s] State transition model building failed due to an internal error.""",uid)
        
          val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          context.parent ! BuildFailed(params)
          
          context.stop(self)
            
        }
	    
      }
       
    }
  
    case event:StatusEvent => {
      
      requestCtx.listener ! String.format("""[INFO][UID: %s] State transition model building finished.""",event.uid)

      /*
       * The StatusEvent message is sent by the RemoteSupervisor (child) and indicates
       * that the (remote) state transition modeling process has been finished successfully.
       * 
       * Due to this message, the DataPipeline actor (parent) is informed about this event
       * and finally this actor stops itself
       */  
      val params = Map(Names.REQ_UID -> event.uid,Names.REQ_MODEL -> "STM")
      context.parent ! BuildFinished(params)
      
      context.stop(self)
      
    }
    
  }

}