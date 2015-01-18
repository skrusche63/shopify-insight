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
import org.apache.spark.sql.SQLContext

import akka.actor.ActorRef

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.elastic._

import de.kp.shopify.insight.model._

import scala.collection.mutable.{Buffer,HashMap}

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.XContentBuilder

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.elasticsearch.index.query.QueryBuilder

class RequestContext(
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

  val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  val sqlCtx = new SQLContext(sparkContext)

  /*
   * Determine Shopify access parameters from the configuration file 
   * (application.conf). Configuration is the accessor to this file.
   */
  private val (endpoint, apikey, password) = Configuration.shopify
  private val storeConfig = new StoreConfig(endpoint,apikey,password)  
  /*
   * This is the reference to the Shopify REST client
   */
  private val shopifyClient = new ShopifyClient(storeConfig)
  
  private val elasticClient = new ElasticClient()
  /*
   * The RemoteContext enables access to remote Akka systems and their actors;
   * this variable is used to access the engines of Predictiveworks
   */
  private val remoteContext = new RemoteContext()
  /*
   * Heartbeat & timeout configuration in seconds
   */
  private val (heartbeat, time) = Configuration.heartbeat  
  /**
   * The base directory for all file based IO
   */
  def getBase = Configuration.input(0)
  
  def getConfig = Configuration
  
  def getESConfig = Configuration.elastic
  
  /**
   * The time interval for schedulers (e.g. StatusSupervisor) to 
   * determine how often alive messages have to be sent
   */
  def getHeartbeat = heartbeat
  
  def getRemoteContext = remoteContext
  
  def getShopifyConfig = storeConfig
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
   * The subsequent methods wrap the respective methods from the Elasticsearch
   * client and makes access to the multiple search indexes available from the
   * request context
   */  
  def createIndex(index:String,mapping:String,topic:String):Boolean 
    = elasticClient.createIndex(index,mapping,topic)

  def count(index:String,mapping:String,query:QueryBuilder):Int 
    = elasticClient.count(index,mapping,query)

  def find(index:String,mapping:String,query:QueryBuilder):SearchResponse 
    = elasticClient.find(index,mapping,query)

  def find(index:String,mapping:String,query:QueryBuilder,size:Int):SearchResponse 
    = elasticClient.find(index,mapping,query,size)

  def getAsMap(index:String,mapping:String,id:String):java.util.Map[String,Object] 
    = elasticClient.getAsMap(index,mapping,id)

  def getAsString(index:String,mapping:String,id:String):String 
    = elasticClient.getAsString(index,mapping,id)

  def putSource(index:String,mapping:String,source:XContentBuilder):Boolean 
    = elasticClient.putSource(index,mapping,source)
  
  def putSources(index:String,mapping:String,sources:List[XContentBuilder]):Boolean 
    = elasticClient.putSources(index,mapping,sources)
  
  /**
   * A public method to retrieve Shopify customers from the REST interface;
   * this method is used to synchronize the customer base
   */
  def getCustomers(req_params:Map[String,String]):List[Customer] = {
    
    val customers = Buffer.empty[Customer]
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify customers from the REST interface
     */
    val uid = req_params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve customers count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = shopifyClient.getCustomersCount(req_params)

    listener ! String.format("""[UID: %s] Load total of %s customers from Shopify store.""",uid,count.toString)

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve customers via a paginated approach, retrieving a maximum
       * of 250 customers per request
       */
      val data = req_params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      customers ++= shopifyClient.getCustomers(req_params).map(customer => new ShopifyMapper(this).extractCustomer(apikey,customer))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    listener ! String.format("""[UID: %s] Customers loaded in %s milli seconds.""",uid,(end-start).toString)
 
    customers.toList
    
  }
  /**
   * A public method to retrieve Shopify products from the REST interface;
   * this method is used to synchronize the product base
   */
  def getProducts(req_params:Map[String,String]):List[Product] = {
    
    val products = Buffer.empty[Product]
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify products from the REST interface
     */
    val uid = req_params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve products count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = shopifyClient.getProductsCount(req_params)

    listener ! String.format("""[UID: %s] Load total of %s products from Shopify store.""",uid,count.toString)

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve products via a paginated approach, retrieving a maximum
       * of 250 customers per request
       */
      val data = req_params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      products ++= shopifyClient.getProducts(req_params).map(product => new ShopifyMapper(this).extractProduct(apikey,product))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    listener ! String.format("""[UID: %s] Products loaded in %s milli seconds.""",uid,(end-start).toString)
 
    products.toList
    
  }

  /**
   * A public method to retrieve the Shopify orders of the last 30, 60 
   * or 90 days from the REST interface
   */
  def getOrders(req_params:Map[String,String]):List[Order] = {
    
    val orders = Buffer.empty[Order]
    
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
      orders ++= shopifyClient.getOrders(order_params).map(order => new ShopifyMapper(this).extractOrder(apikey,order))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    listener ! String.format("""[UID: %s] Orders loaded in %s milli seconds.""",uid,(end-start).toString)
 
    orders.toList
    
  }
  
  private def setOrderParams(params:Map[String,String]):Map[String,String] = {

    val data = HashMap.empty[String,String]
    
    /*
     * We restrict to those orders that have been paid,
     * and that are closed already, as this is the basis
     * for adequate forecasts 
     */
    data += "financial_status" -> "paid"
    data += "status" -> "closed"

    data.toMap
    
  }
  
}