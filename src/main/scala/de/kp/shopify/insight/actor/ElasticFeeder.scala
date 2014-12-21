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

import de.kp.shopify.insight.ShopifyContext
import de.kp.shopify.insight.io.OrderMapper

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.source._

import scala.collection.mutable.ArrayBuffer

private case class Pair(time:Long,state:String)

/**
 * The ElasticFeeder retrieves orders or products from a Shopify store and
 * registers them in an Elasticsearch index for subsequent processing. From
 * orders the following indexes are built:
 * 
 * Store:orders --+----> Amount index (orders/amount)
 *                :
 *                :
 *                +----> Item index (orders/items)
 *                :
 *                :
 *                +----> State index (orders/states)
 * 
 */
class ElasticFeeder(listener:ActorRef) extends BaseActor {

  private val stx = new ShopifyContext(listener)  
  
  override def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data(Names.REQ_UID)
      try {
        
        val Array(task,topic) = req.task.split(":")
        topic match {
          /*
           * Retrieve orders from Shopify store via REST interface, prepare index
           * for 'items' and (after that) track each order as 'item'; note, that 
           * this request retrieves all orders that match a certain set of filter
           * criteria; pagination is done inside this request
           */
          case "order" => {
            
            val start = new java.util.Date().getTime
            
            listener ! String.format("""[UID: %s] Request to feed orders received.""",uid)
            
            /*
             * STEP #1: Create search indexes (if not already present)
             *
             * The 'amount' index (mapping) is used by Intent Recognition and
             * supports the Recency-Frequency-Monetary (RFM) model
             * 
             * The 'items' index (mapping) specifies a transaction database and
             * is used by Association Analysis, Series Analysis and othe engines
             * 
             * The 'states' index (mapping) specifies a states database derived 
             * from the amount representation and used by Intent Recognition
             */
            val handler = new ElasticHandler()
            
            if (handler.createIndex(req,"orders","amount","amount") == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")

            if (handler.createIndex(req,"orders","items","item") == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")
 
            if (handler.createIndex(req,"orders","states","state") == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")
 
            listener ! String.format("""[UID: %s] Elasticsearch indexes created.""",uid)

            /*
             * STEP #2: Retrieve orders from a certain shopify store; this request takes
             * into account that the Shopify REST interface returns maximally 250 orders
             */
            val orders = stx.getOrders(req)
            /*
             * STEP #3: Build tracking requests to send the collected orders to
             * the respective service or engine; the orders are sent independently 
             * following a fire-and-forget strategy
             */
            val mapper = new OrderMapper()
            /*
             * The 'amount' perspective of the order is built and registered
             */
            val amounts = orders.map(mapper.toAmountMap(_))

            if (handler.putAmount("orders","amount",amounts) == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")

            listener ! String.format("""[UID: %s] Amount perspective registered in Elasticsearch index.""",uid)
            /*
             * The 'item' perspective of the order is built and registered
             */
            val items = orders.map(mapper.toItemMap(_))
            
            if (handler.putItems("orders","items",items) == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")
          
            listener ! String.format("""[UID: %s] Item perspective registered in Elasticsearch index.""",uid)
            /*
             * The 'state' perspective of the order is built and registered
             */
            val states = toStates(orders.map(mapper.toAmountTuple(_)))
            
            if (handler.putItems("orders","states",states) == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")
          
            listener ! String.format("""[UID: %s] State perspective registered in Elasticsearch index.""",uid)

            val end = new java.util.Date().getTime
            listener ! String.format("""[UID: %s] Order tracking finished in %s ms.""",uid,(end-start).toString)
         
          }
          
          case "product" => throw new Exception("Product tracking is not supported yet.")
            
          case _ => {/* do nothing */}
          
        }

      } catch {
        case e:Exception => listener ! String.format("""[UID: %s] Tracking request exception: %s.""",uid,e.getMessage)

      } finally {
        
        context.stop(self)
        
      }
      
    }
  
  }

  private def toStates(amounts:List[(String,String,Long,Float)]):List[Map[String,String]] = {
    /*
     * Group amounts by site & user and restrict to those
     * users with more than one purchase
     */
    amounts.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).flatMap(p => {

      val (site,user) = p._1
      val orders = p._2.map(v => (v._3,v._4)).toList.sortBy(_._1)
      
      /* Extract first order */
      var (pre_time,pre_amount) = orders.head
      val states = ArrayBuffer.empty[Pair]

      for ((time,amount) <- orders.tail) {
        
        val astate = AmountHandler.stateByAmount(amount,pre_amount)
        val tstate = AmountHandler.stateByTime(time,pre_time)
      
        val state = astate + tstate
        states += Pair(time,state)
        
        pre_amount = amount
        pre_time   = time
        
      }
      
      states.map(x => Map(
        Names.SITE_FIELD -> site,
        Names.USER_FIELD -> user,
          
        Names.STATE_FIELD -> x.state,
        Names.TIMESTAMP_FIELD -> x.time.toString
     ))
      
    }).toList
    
  }

}