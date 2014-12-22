package de.kp.shopify.insight
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

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io.OrderBuilder

import scala.collection.mutable.ArrayBuffer

class ShopifyContext(listener:ActorRef) {

  private val (endpoint,apikey,password) = Configuration.shopify
  /*
   * The 'apikey' is used as the 'site' parameter when indexing
   * Shopify data with Elasticsearch
   */
  private val conf = new ShopifyConfiguration(endpoint,apikey,password)  
  private val client = new ShopifyClient(conf)

  
  def getPurchases(req:ServiceRequest):List[AmountObject] = getPurchases(req.data)
  
  /**
   * This method retrieves orders from a Shopify store via the REST API
   * and transforms each order into an 'AmountObject'
   */
  def getPurchases(params:Map[String,String]):List[AmountObject] = {
    
    val start = new java.util.Date().getTime
    
    val uid = params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve orders count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = client.getOrdersCount(params)

    listener ! String.format("""[UID: %s] Load total of %s orders from Shopify store.""",uid,count.toString)

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")

    val purchases = ArrayBuffer.empty[AmountObject]
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve orders via a paginated approach, retrieving a maximum
       * of 250 orders per request
       */
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      purchases ++= client.getOrders(params).map(order => new OrderBuilder().extractPurchase(apikey,order))
             
       page += 1
              
    }
    
    val end = new java.util.Date().getTime
    listener ! String.format("""[UID: %s] Purchases loaded in %s milli seconds.""",uid,(end-start).toString)

    purchases.toList
    
  }
  
  def getOrders(req:ServiceRequest):List[Order] = getOrders(req.data)

  def getOrders(params:Map[String,String]):List[Order] = {
    
    val start = new java.util.Date().getTime
    
    val uid = params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve orders count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = client.getOrdersCount(params)

    listener ! String.format("""[UID: %s] Load total of %s orders from Shopify store.""",uid,count.toString)

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")

    val orders = ArrayBuffer.empty[Order]
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve orders via a paginated approach, retrieving a maximum
       * of 250 orders per request
       */
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      orders ++= client.getOrders(params).map(order => new OrderBuilder().extractOrder(apikey,order))
             
       page += 1
              
    }

    val end = new java.util.Date().getTime
    listener ! String.format("""[UID: %s] Orders loaded in %s milli seconds.""",uid,(end-start).toString)

    orders.toList
    
  }

  def getProduct(pid:Long) = client.getProduct(pid)

}