package de.kp.shopify.insight.build
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

import de.kp.shopify.insight.actor.BaseActor

/**
 * AAEBuilder is responsible for building an associaton rule model
 * by invoking the Association Analysis engine of Predictiveworks.
 * 
 * This is part of the 'build' sub process that represents the second
 * component of the data analytics pipeline. Note, that the DataPipeline
 * actor is the PARENT actor of this actor
 * 
 */
class AAEBuilder(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {
  
  private val config = ctx.getConfig
  
  override def receive = {
   
    case message:StartBuild => {
      
      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      val start = new java.util.Date().getTime.toString            
      ctx.listener ! String.format("""[INFO][UID: %s] %s mining request received at %s.""",uid,name,start)
      
      /* 
       * Build service request message to invoke remote Association Analysis 
       * engine to train an association rule model from an 'items' index
       */
      val service = "association"
      val task = "train"

      val data = new AAEHandler().train(req_params)
      val req  = new ServiceRequest(service,task,data)
      
      val serialized = Serializer.serializeRequest(req)
      val response = ctx.getRemoteContext.send(service,serialized).mapTo[String]  
      
      ctx.listener ! String.format("""[INFO][UID: %s] %s mining started.""",uid,name)
      
      /*
       * The RemoteSupervisor actor monitors the Redis cache entries of this
       * association rule mining request and informs this actor (as parent)
       * that a certain status has been reached
       */
      val status = ResponseStatus.MINING_FINISHED
      val supervisor = context.actorOf(Props(new Supervisor(req,status,config)))
      
      /*
       * We evaluate the response message from the remote Association Analysis 
       * engine to check whether an error occurred
       */
      response.onSuccess {
        
        case result => {
 
          val res = Serializer.deserializeResponse(result)
          if (res.status == ResponseStatus.FAILURE) {
      
            ctx.listener ! String.format("""[ERROR][UID: %s] %s mining failed due to an engine error.""",uid,name)
 
            context.parent ! BuildFailed(res.data)
            context.stop(self)

          }
         
        }

      }
      response.onFailure {
          
        case throwable => {
      
          ctx.listener ! String.format("""[ERROR][UID: %s] %s mining failed due to an internal error.""",uid,name)
        
          val res_params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ req_params
          context.parent ! BuildFailed(res_params)
          
          context.stop(self)
            
          }
	    }
       
    }
  
    case event:StatusEvent => {
      
      val uid = params(Names.REQ_UID)
      val name = params(Names.REQ_NAME)
      
      val end = new java.util.Date().getTime.toString            
      ctx.listener ! String.format("""[INFO][UID: %s] %s mining finished at %s.""",event.uid,name,end)

      val res_params = Map(Names.REQ_UID -> event.uid,Names.REQ_MODEL -> name)
      context.parent ! BuildFinished(res_params)
      
      context.stop(self)
      
    }
    
  }

}