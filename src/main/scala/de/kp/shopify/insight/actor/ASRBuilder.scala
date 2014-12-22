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

import akka.actor.{ActorRef,Props}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.{RemoteContext}

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

/**
 * ASRBuilder is responsible for building an associaton rule model
 * by invoking the Association Analysis engine of Predictiveworks.
 * 
 * This is part of the 'build' sub process that represents the second
 * component of the data analytics pipeline.
 * 
 */
class ASRBuilder(rtx:RemoteContext,listener:ActorRef) extends BaseActor {
  
  override def receive = {
   
    case message:StartBuild => {
      /* 
       * Build service request message to invoke remote Association Analysis 
       * engine to train an association rule model from an 'items' index
       */
      val service = "association"
      val task = "train"

      val data = new ASRHandler().train(message.data)
      val req  = new ServiceRequest(service,task,data)
      
      val serialized = Serializer.serializeRequest(req)
      val response = rtx.send(service,serialized).mapTo[String]  
      /*
       * The RemoteSupervisor actor monitors the Redis cache entries of this
       * association rule mining request and informs this actor (as parent)
       * that a certain status has been reached
       */
      val status = ResponseStatus.MINING_FINISHED
      val supervisor = context.actorOf(Props(new RemoteSupervisor(req,status)))
      
      /*
       * We evaluate the response message from the remote
       * Association Analysis engine to check whether an
       * error occurred
       */
      response.onSuccess {
        
        case result => {
 
          val res = Serializer.deserializeResponse(result)
          if (res.status == ResponseStatus.FAILURE) {
 
            context.parent ! BuildFailed(res.data)
            context.stop(self)

          }
         
        }

      }
      response.onFailure {
          
        case throwable => {
        
          val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          context.parent ! BuildFailed(params)
          
          context.stop(self)
            
          }
	    }
       
    }
  
    case event:StatusEvent => {
      /*
       * The StatusEvent message is sent by the RemoteSupervisor (child) and indicates
       * that the (remote) association rule mining process has been finished successfully.
       * 
       * Due to this message, the Pipeliner actor (parent) is informed about this event
       * and finally this actor stops itself
       */  
      val params = Map(Names.REQ_UID -> event.uid,Names.REQ_MODEL -> "ASR")
      context.parent ! BuildFinished(params)
      
      context.stop(self)
      
    }
    
  }

}