package de.kp.shopify.insight.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

case class ActorInfo(
  name:String,timestamp:Long
)

case class ActorStatus(
  name:String,date:String,status:String
)

case class ActorsStatus(items:List[ActorStatus])

case class StopActor()

/**
 * StartPipeline specifies a request message sent to a data analytics pipeline 
 * (see DataPipeline actor) to start a new data mining and model building process
 */
case class StartPipeline(data:Map[String,String])

case class SimpleResponse(uid:String,message:String)

/****************************************************************************
 * 
 *                      SUB PROCESS 'SYNCHRONIZE'
 * 
 ***************************************************************************/

case class StartSynchronize(data:Map[String,String])

case class SynchronizeFailed(data:Map[String,String])

case class SynchronizeFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'COLLECT'
 * 
 ***************************************************************************/

/**
 * StartPrepare specifies a message sent to a data preparer actor to indicate 
 * that the preparation sub process has to be started
 */
case class StartPrepare(data:Map[String,String])

case class PrepareFailed(data:Map[String,String])

/**
 * PrepareFinished specifies a message sent to the DataPipeline actor to 
 * indicate that the data preparation sub process finished sucessfully
 */
case class PrepareFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'BUILD'
 * 
 ***************************************************************************/

/**
 * StartBuildspecifies a message sent from the DataPipeline actor to the remote 
 * builder to initiate a data mining or model building task
 */
case class StartBuild(data:Map[String,String])

case class BuildFailed(data:Map[String,String])

case class BuildFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'ENRICH'
 * 
 ***************************************************************************/

case class StartEnrich(data:Map[String,String])

case class EnrichFailed(data:Map[String,String])

case class EnrichFinished(data:Map[String,String])

/****************************************************************************
 * 
 *                      SUB PROCESS 'PROFILE'
 * 
 ***************************************************************************/

case class StartProfile(data:Map[String,String])

case class ProfileFailed(data:Map[String,String])

case class ProfileFinished(data:Map[String,String])

case class Customer(
  /* 
   * The 'apikey' of the Shopify cloud service is used as a
   * unique identifier for the respective tenant or website
   */
  site:String,
  /*
   * Unique identifier that designates a certain Shopify
   * store customer
   */
  id:String,
  /*
   * The first and last name of the customer to be used
   * when visualizing computed customer information
   */
  firstName:String,
  lastName:String,
  /*
   * THe signup date of the customer
   */
  created_at:String,
  
  /*
   * The email address of a customer and a flag to indicate,
   * whether this address is verified; this is relevant for
   * email marketing
   */
  emailAddress:String,
  emailVerified:Boolean,
  /*
   * A flag that indicates whether the customer accepts
   * marketing or not
   */
  marketing:Boolean,
  /*
   * The state of the customer, i.e. 'disabled' or 'enabled'
   */ 
  state:String

)

case class Image(
  /*
   * Unique identifier that designates a certain Shopify
   * store product image
   */
  id:String,

  position:Int,
  src:String
)

case class Location(
    
  countryname:String,
  countrycode:String,

  region:String,
  regionname:String,
  
  areacode:Int,
  dmacode:Int,
  
  metrocode:Int,
  city:String,
  
  postalcode:String,
	  
  lat:Float,
  lon:Float

)

case class Product(
  /* 
   * The 'apikey' of the Shopify cloud service is used as a
   * unique identifier for the respective tenant or website
   */
  site:String,
  /*
   * Unique identifier that designates a certain Shopify
   * store product
   */
  id:String,
  /*
   * The category assigned to a certain product, this field
   * can be used to group similar products or to determine
   * customer preferences
   */
  category:String,
  /*
   * The name of a certain product
   */
  name:String,

  vendor:String,

  /*
   * 'tags' describes a comma separated list of keywords
   * that describe a certain product
   */
  tags:String,

  images:List[Image]

)

/**
 * OrderItem is used to describe a single order or purchase
 * related entity that is indexed in an Elasticsearch index 
 * for later mining and prediction tasks
 */
case class OrderItem(
  /*
   * Unique identifier to determine a Shopify product
   */
  item:Int,
  /*
   * The name of a certain product; this information is
   * to describe the mining & prediction results more 
   * user friendly; the name is optional
   */
  name:String = "",
  /*
   * The number of items of a certain product within
   * an order; the quantity is optional; if not provided,
   * the item is considered only once in the respective
   * market basket analysis
   */
  quantity:Int = 0,
  /*
   * Currency used with the respective order    
   */     
  currency:String = "USD",
  /*
   * Price of the item 
   */
  price:String = "",
  /*
   * SKU 
   */
  sku:String = ""
  
)
case class Order(
  /* 
   * The 'apikey' of the Shopify cloud service is used as a
   * unique identifier for the respective tenant or website
   */
  site:String,
  /*
   * Unique identifier that designates a certain Shopify
   * store customer (see ShopifyCustomer) 
   */
  user:String,
  /*
   * The IP address that is assigne to an online order;
   * this field is leveraged to determine location data
   */
  ip_address:String,
  /*
   * The user agent for the online access; this field is
   * leveraged to determine the referrer and others
   */
  user_agent:String,
  /*
   * The timestamp for a certain Shopify order
   */
  timestamp:Long,
  /*
   * The group identifier is equal to the order identifier
   * used in Shopify orders
   */
  group:String,
  /*
   * The total amount of a certain purchase or transaction
   */
  amount:Float,
  /*
   * The items in an order
   */ 
  items:List[OrderItem]
)
case class Orders(items:List[Order])

/****************************************************************************
 * 
 *                      PROCESS 'QUERY'
 * 
 ***************************************************************************/

/**
 * An AggregateQuery retrieves aggregated data for all orders or purchase
 * transactions within a certain period of time
 */
case class AggregateQuery(data:Map[String,String])

case class ForecastQuery(data:Map[String,String])

case class LoyaltyQuery(data:Map[String,String])

case class ProductQuery(data:Map[String,String])

case class RecommendationQuery(data:Map[String,String])

/**
 * A TaskQuery retrieves metadata for all registered preparation and synchronization
 * tasks processed by the insight server
 */
case class TaskQuery(data:Map[String,String])

case class UserQuery(data:Map[String,String])

/**
 * A suggestion is a list of weighted (by support & confidence) 
 * product suggestions, that helps to build custom collections
 */
case class Suggestion(products:List[ShopifyProduct],support:Int,confidence:Double)
case class Suggestions(items:List[Suggestion])

case class Products(products:List[ShopifyProduct])

/**
 * A forecast (event) claims a certain purchase amount within a specific
 * period of days, and the assigned score describes the likelihood that
 * this event happens
 */
case class Forecast(site:String,user:String,amount:Float,days:Int,score:Double)

case class Forecasts(items:List[Forecast])

case class Recommendation(
  /* A recommendation is described on a per user basis */
  site:String,user:String,products:List[ShopifyProduct]
)

case class Recommendations(items:List[Recommendation])

object ResponseStatus extends BaseStatus

object Sources {
  
  val ELASTIC:String = "ELASTIC"
    
}


object Serializer extends BaseSerializer {
  def serializeActorsStatus(statuses:ActorsStatus):String = write(statuses) 
}

object Messages extends BaseMessages
