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

import akka.actor.{Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.routing.RoundRobinRouter

import de.kp.shopify.insight.ShopifyContext
import de.kp.shopify.insight.model._

import scala.concurrent.duration.DurationInt

class FeedMaster(name:String) extends MonitoredActor(name) {
  /*
   * The ShopifyMaster has to perform two subsequent tasks; first the
   * request specific data must be retrieved from a Shopify store, and
   * second the respective data have to indexed in an Elasticsearch index
   */ 

  protected val shopifyCtx = new ShopifyContext()
  protected val collector = context.actorOf(Props(new CollectWorker(shopifyCtx)).withRouter(RoundRobinRouter(workers)))
  
  override def receive = {
    /*
     * Message sent by the scheduler to track the 'heartbeat' of this actor
     */
    case req:AliveMessage => register(name)

    case req:ServiceRequest => {
      
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
	  
	  req.task.split(":")(0) match {
	  
        case "collect" => {
          /*
           * We send the request to the 
           */
          //TODO
          
        }
        
        case "index" => {
          /*
           * This task specifies the second task for the ShopifyMaster and
           * performs indexing of data collected from a Shopify store
           */
          val response = ask(router, req).mapTo[ServiceResponse]
      
          response.onSuccess {
            case result => origin ! result
          }
          response.onFailure {
            case result => origin ! failure(req)      
	      }
        
        }
        
        case _ => 
      
      }
    
    }
    
    case _ => {}
  
  }
}