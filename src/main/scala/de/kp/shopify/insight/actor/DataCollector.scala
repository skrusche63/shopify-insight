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

import de.kp.shopify.insight._

import de.kp.shopify.insight.analytics._
import de.kp.shopify.insight.elastic._

import de.kp.shopify.insight.model._

/**
 * The DataCollector retrieves orders or products from a Shopify store and
 * registers them in an Elasticsearch index for subsequent processing. From
 * orders the following indexes are built:
 * 
 * Store:orders --+----> Item index (orders/items)
 *                :
 *                :
 *                +----> State index (orders/states)
 * 
 */
class DataCollector(requestCtx:RequestContext) extends BaseActor {
  
  override def receive = {
    
    case msg:StartCollect => {

      val req_params = msg.data
      val uid = req_params(Names.REQ_UID)
      
      try {
        
        val topic = req_params(Names.REQ_TOPIC)
        topic match {
          /*
           * Retrieve orders from Shopify store via REST interface, prepare index
           * for 'items' and (after that) track each order as 'item'; note, that 
           * this request retrieves all orders that match a certain set of filter
           * criteria; pagination is done inside this request
           */
          case "order" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] Order indexing started.""",uid)
            
            val start = new java.util.Date().getTime            

            /*
             * STEP #1: Retrieve orders from a certain shopify store; this request takes
             * into account that the Shopify REST interface returns maximally 250 orders
             */
            val orders = requestCtx.getOrders(req_params)
            /*
             * STEP #2: Index 'items' and 'states' to enable remote Predictiveworks 
             * engines to access these data; note, that 'items' are used by Association
             * Analysis and 'states' by Intent Recognition
             */
            val analytics = new Analytics(requestCtx)

            val aggregate = analytics.buildSUM(req_params,orders)
            
            if (requestCtx.putSource("orders","aggregates",aggregate) == false)
              throw new Exception("Indexing for 'orders/aggregates' has been stopped due to an internal error.")
          
            requestCtx.listener ! String.format("""[INFO][UID: %s] Item perspective registered in Elasticsearch index.""",uid)

            val items = analytics.buildITM(req_params,orders)
            
            if (requestCtx.putSources("orders","items",items) == false)
              throw new Exception("Indexing for 'orders/items' has been stopped due to an internal error.")
          
            requestCtx.listener ! String.format("""[INFO][UID: %s] Item perspective registered in Elasticsearch index.""",uid)

            /*
             * The RFM model has a focus on the monetary and temporal dimension
             * of purchase transactions (orders), and, besides static data such
             * as averages, minimums and maximums, specifies the transition from
             * one transaction to another
             */
            val states = analytics.buildRFM(req_params,orders)
            
            if (requestCtx.putSources("orders","states",states) == false)
              throw new Exception("Indexing for 'orders/states' has been stopped due to an internal error.")
          
            requestCtx.listener ! String.format("""[INFO][UID: %s] State perspective registered in Elasticsearch index.""",uid)

            val end = new java.util.Date().getTime
            requestCtx.listener ! String.format("""[INFO][UID: %s] Order indexing finished in %s ms.""",uid,(end-start).toString)
         
            /*
             * Finally the pipeline gets informed, that the collection 
             * sub process finished successfully
             */
            context.parent ! CollectFinished(req_params)
            
          }
          
          case "product" => throw new Exception("Product collection is not supported yet.")
            
          case _ => {/* do nothing */}
          
        }

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Collection exception: %s.""",uid,e.getMessage)
          context.parent ! CollectFailed(req_params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
      
    }
  
  }

}