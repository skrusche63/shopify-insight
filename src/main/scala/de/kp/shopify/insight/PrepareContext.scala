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

import org.apache.spark.SparkContext
import akka.actor.ActorRef

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.model._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.{Buffer,HashMap}

class PrepareContext(
  /*
   * Reference to the common SparkContext; this context can be used
   * to access HDFS based data sources or leverage the Spark machine
   * learning library or other Spark based functionality
   */
  @transient val sparkContext:SparkContext, 
   /*
    * This is a specific actor instance, assigned to the global actor
    * system, that is responsible for receiving any kind of messages
    */
   val listener:ActorRef) extends Serializable {

  /*
   * Determine Shopify access parameters from the configuration file 
   * (application.conf). Configuration is the accessor to this file.
   */
  private val (endpoint, apikey, password) = Configuration.shopify
  private val shopifyConfig = new ShopifyConfiguration(endpoint,apikey,password)  
  /*
   * This is the reference to the Shopify REST client
   */
  private val shopifyClient = new ShopifyClient(shopifyConfig)
  
  /*
   * The RemoteContext enables access to remote Akka systems and their actors;
   * this variable is used to access the engines of Predictiveworks
   */
  private val remoteContext = new RemoteContext()
  /*
   * Reference to the Shopify orders of the last 30, 60 or 90 days; as these
   * data are used more than once, the server context is an appropriate place
   * to keep them transiently
   */
  private val shopifyOrders = Buffer.empty[Order]
  /*
   * Heartbeat & timeout configuration in seconds
   */
  private val (heartbeat, time) = Configuration.heartbeat     

  /**
   * The time interval for schedulers (e.g. MonitoredActor or StatusSupervisor) to 
   * determine how often alive messages have to be sent
   */
  def getHeartbeat = heartbeat
  
  def getRemoteContext = remoteContext
  
  def getShopifyConfig = shopifyConfig
  /*
   * The 'apikey' is used as the 'site' parameter when indexing
   * Shopify data with Elasticsearch
   */
  def getSite = apikey
  
  /**
   * The timeout interval used to supervise actor interaction
   */
  def getTimeout = time

  /**
   * Reset all memory consuming data structures
   */
  def clear {
    shopifyOrders.clear
  }
  /**
   * The internal order buffer MUST be cleared when a new data analytics
   * pipeline starts
   */
  def clearOrders = shopifyOrders.clear
  /**
   * A public method to retrieve the Shopify orders of the last 30, 60 
   * or 90 days from the REST interface
   */
  def getOrders(req_params:Map[String,String]):List[Order] = {
    
    if (shopifyOrders.isEmpty == false) return shopifyOrders.toList
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify orders from the last 30, 60 or 90 days from
     * the respective REST interface
     */
    val order_params = req_params ++ setOrderParams(req_params)
    val uid = order_params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve orders count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = shopifyClient.getOrdersCount(order_params)

    listener ! String.format("""[UID: %s] Load total of %s orders from Shopify store.""",uid,count.toString)

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve orders via a paginated approach, retrieving a maximum
       * of 250 orders per request
       */
      val data = order_params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      shopifyOrders ++= shopifyClient.getOrders(order_params).map(order => new OrderBuilder().extractOrder(apikey,order))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    listener ! String.format("""[UID: %s] Orders loaded in %s milli seconds.""",uid,(end-start).toString)
 
    shopifyOrders.toList
    
  }
  
  /**
   * This method transforms retrieved orders into a list of purchases;
   * a 'purchase' is represented by an AmountObject
   */
  def getPurchases(params:Map[String,String]):List[AmountObject] = {
    
    if (shopifyOrders.isEmpty) return List.empty[AmountObject]
    /* Transform orders into AmountObject representation */
    shopifyOrders.map(x => AmountObject(x.site,x.user,x.timestamp,x.amount)).toList
    
  }
  
  private def setOrderParams(params:Map[String,String]):Map[String,String] = {

    val days = if (params.contains(Names.REQ_DAYS)) params(Names.REQ_DAYS).toInt else 30
    
    val created_max = new DateTime()
    val created_min = created_max.minusDays(days)

    val data = HashMap(
      "created_at_min" -> formatted(created_min.getMillis),
      "created_at_max" -> formatted(created_max.getMillis)
    )
    
    /*
     * We restrict to those orders that have been paid,
     * and that are closed already, as this is the basis
     * for adequate forecasts 
     */
    data += "financial_status" -> "paid"
    data += "status" -> "closed"

    data.toMap
    
  }
  /**
   * This method is used to format a certain timestamp, provided with 
   * a request to collect data from a certain Shopify store
   */
  private def formatted(time:Long):String = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
    
    formatter.print(time)
    
  }
  
}