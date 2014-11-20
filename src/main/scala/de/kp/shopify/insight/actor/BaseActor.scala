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

import akka.actor.{Actor,ActorLogging}
import de.kp.shopify.insight.model._

import scala.concurrent.Future

abstract class BaseActor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      try {
      
        val service = req.service
        val message = Serializer.serializeRequest(req)
      
        val response = getResponse(service,message)     
        response.onSuccess {
          case result => origin ! Serializer.deserializeResponse(result)
        }
        response.onFailure {
          case throwable => origin ! failure(req,throwable.getMessage)	 	      
	    }
      
      } catch {
        case e:Exception => origin ! failure(req,e.getMessage)  
      }
    }
    
  }
  
  protected def failure(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")    
    new ServiceResponse(req.service,req.task,Map("uid" -> uid),ResponseStatus.FAILURE)	
  
  }
  
  protected def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val uid = req.data("uid")    
    new ServiceResponse(req.service,req.task,Map("uid" -> uid,"message" -> message),ResponseStatus.FAILURE)	
  
  }

  protected def getResponse(service:String,message:String):Future[String]

}