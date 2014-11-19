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

import de.kp.shopify.insight.ShopifyContext
import de.kp.shopify.insight.model._

import scala.concurrent.Future

class CollectWorker(ctx:ShopifyContext) extends BaseActor {
  
  override def receive = {
    
    case req:ServiceRequest => {
      /*
       * This request is sent by the FeedMaster; after having 
       * retrieved and transformed the data from a certain store,
       * the sender is informed to index the respective data
       */
      val origin = sender

      try {
        /*
         * Retrieve orders from a certain shopify store and
         * convert them into an internal format 
         */
        val orders = ctx.getOrders(req)
        /*
         * Send converted orders to sender and index them
         * in an Elasticsearch index
         */
        
        // TODO
        
        
      } catch {
        case e:Exception => origin ! failure(req,e.getMessage)

      }
      
    }
    
  }

  /**
   * This method is not used by this actor
   */
  override def getResponse(service:String,message:String) = null

}