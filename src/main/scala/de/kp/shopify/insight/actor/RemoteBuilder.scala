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

import akka.actor.ActorRef

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.{RemoteContext}

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

class RemoteBuilder(ctx:RemoteContext,listener:ActorRef) extends BaseActor {
  
  override def receive = {
   
    case req:ServiceRequest => {
      
      val origin = sender
      
      val uid = req.data(Names.REQ_UID)
      val Array(task,topic) = req.task.split(":")
      
      try {
      
        val (service,message) = topic match {
        
          case "cross-sell" => {
            /* 
             * Build service request message to invoke remote Association Analysis engine; 
             * the 'cross-sell' request is mapped onto a 'train' task
             */
            val service = "association"
            val task = "train"

            val data = new CrossSellBuilder().train(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case "loyalty" => {
            /*
             * Build service request message to invoke remote Intent Recognition engine;
             * the 'loyalty' request is mapped onto a 'train' task
             */
            val service = "intent"
            val task = "train"

            val data = new LoyaltyBuilder().train(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case "placement" => {
             /* 
             * Build service request message to invoke remote Association Analysis engine; 
             * the 'placement' request is mapped onto a 'train' task
             */
            val service = "association"
            val task = "train"

            val data = new PlacementBuilder().train(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
         }
        
          case "purchase" => {
            /*
             * Build service request message to invoke remote Intent Recognition engine;
             * the 'purchase' request is mapped onto a 'train' task
             */
            val service = "intent"
            val task = "train"

            val data = new PurchaseBuilder().train(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case "recommendation" => {
             /* 
             * Build service request message to invoke remote Association Analysis engine; 
             * the 'recommendation' request is mapped onto a 'train' task
             */
            val service = "association"
            val task = "train"

            val data = new RecommendationBuilder().train(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case _ => throw new Exception(String.format("""[UID: %s] Unknown topic received.""",uid))
        
        }
        
        val response = getResponse(service,message)     
        response.onSuccess {
        
          case result => {
 
            val intermediate = Serializer.deserializeResponse(result)
            origin ! buildResponse(req,intermediate)
        
          }

        }
        response.onFailure {
          case throwable => origin ! failure(req,throwable.getMessage)	 	      
	    }
      
      } catch {
        case e:Exception => origin ! failure(req,e.getMessage)
        
      }
      
    }
  
  }

  private def getResponse(service:String,message:String) = ctx.send(service,message).mapTo[String] 

  private def buildResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    null
  }

}