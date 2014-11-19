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

import de.kp.shopify.insight.{RemoteContext,ShopifyContext}
import de.kp.shopify.insight.model._

class FeedWorker(ctx:RemoteContext) extends WorkerActor(ctx) {
  
  private val stx = new ShopifyContext()  
  override def receive = {
    
    case req:ServiceRequest => {

      val origin = sender
      val service = req.service
      
      try {
        
        req.task.split(":")(1) match {
          
          case "order" => {
            /*
             * Retrieve orders from a certain shopify store and
             * convert them into an internal format 
             */
            val orders = stx.getOrders(req)

            /*
             * Build tracking request to send the collected orders
             * to the selected service or engine
             */
            val request:ServiceRequest = null // TODO           
            
            val message = Serializer.serializeRequest(request)
      
            val response = getResponse(service,message)     
            response.onSuccess {
              case result => origin ! Serializer.deserializeResponse(result)
            }
            response.onFailure {
              case result => origin ! failure(req)	 	      
	        }
            
          }
          
          case "product" => {
            
            // TODO
            
          }
            
          case _ => {/* do nothing */}
          
        }
        
      } catch {
        case e:Exception => origin ! failure(req,e.getMessage)

      }
      
    }
    
  }

}