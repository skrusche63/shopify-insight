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

import de.kp.shopify.insight.model._

import scala.concurrent.Future

class Feeder(name:String,listener:ActorRef) extends MasterActor(name) {

  override def execute(req:ServiceRequest):Future[Any] = {

    val uid = req.data(Names.REQ_UID)      
    val source = req.data(Names.REQ_SOURCE)
    /*
     * Feeding orders or products from a Shopify store a certain data
     * endpoint (e.g. an Elasticsearch index) can be longer running
     * task; to this end a fire-and-forget approach is used here.
     */
    val response = try {      
      source match {
        
        case Sources.ELASTIC => {
          /*
           * Send request to ElasticFeeder actor and inform requestor
           * that the tracking process has been started. Error and
           * interim messages of this process are sent to the listener
           */
          val actor = context.actorOf(Props(new ElasticFeeder(listener)))
          actor ! req
          /*
           * This message is sent directly after the request has been
           * delegated to the ElasticFeeder
           */
          val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.TRACKING_STARTED(uid))    
          new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
          
        }
        
        case _ => throw new Exception(String.format("""[UID: %s] The source '%s' is not supported.""",uid,source))
      
      }
    
    } catch {
      case e:Exception => failure(req,e.getMessage)

    } 
    
    Future {response}
    
  }
  
}