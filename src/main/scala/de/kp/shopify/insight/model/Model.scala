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

case class AliveMessage()

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
/**
 * Status event specifies a message sent by the RemoteSupervisor actor to indicate
 * that a certain 'status' of a mining or model building task has been reached.
 */
case class StatusEvent(uid:String,service:String,task:String,value:String)

case class SimpleResponse(uid:String,message:String)

/****************************************************************************
 * 
 *                      SUB PROCESS 'COLLECT'
 * 
 ***************************************************************************/

/**
 * StartCollect specifies a message sent to a data collector actor (e.g. Elastic
 * Collector) to indicate that the collection sub process has to be started
 */
case class StartCollect(data:Map[String,String])

case class CollectFailed(data:Map[String,String])

/**
 * CollectFinished specifies a message sent to the DataPipeline actor to indicate
 * that the data collection sub process finished sucessfully
 */
case class CollectFinished(data:Map[String,String])

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

/**
 * A suggestion is a list of weighted (by support & confidence) 
 * product suggestions, that helps to build custom collections
 */
case class Suggestion(products:List[ShopifyProduct],support:Int,confidence:Double)
case class Suggestions(items:List[Suggestion])

case class Products(products:List[ShopifyProduct])

/**
 * An forecast claims a certain purchase amount at a specific
 * timestamp with an assigned score, that describes the likelihood 
 * that this purchase happens
 */
case class Forecast(amount:Float,timestamp:Long,score:Double)
/**
 * A user forecast collects a list of forecasts with respect
 * to a certain user
 */
case class UserForecast(site:String,user:String,steps:List[Forecast])

case class UserForecasts(items:List[UserForecast])

case class Recommendation(
  /* A recommendation is described on a per user basis */
  site:String,user:String,products:List[ShopifyProduct]
)

case class Recommendations(items:List[Recommendation])

/**
 * A derived association rule that additionally specifies the matching weight
 * between the antecent field and the respective field in mined and original
 * association rules
 */
case class WeightedRule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double,weight:Double)
/**
 * A set of weighted rules assigned to a certain user of a specific site
 */
case class UserRules(site:String,user:String,items:List[WeightedRule])

case class MultiUserRules(items:List[UserRules])

object ResponseStatus extends BaseStatus

object Sources {
  
  val ELASTIC:String = "ELASTIC"
    
}

object Sinks {
  
  val ELASTIC:String = "ELASTIC"
    
}

object Statuses {
  
  val OPEN:String = "open"
  val CLOSED:String = "closed"
  
  val CANCELLED:String = "cancelled"
  val ANY:String = "any"

  private val statuses = List(OPEN,CLOSED,CANCELLED,ANY)
  def isStatus(status:String):Boolean = statuses.contains(status)
  
}
object Serializer extends BaseSerializer {

  def serializeActorsStatus(statuses:ActorsStatus):String = write(statuses) 
  
  /** 
   * Multi user rules specify the result of association analysis and are used to 
   * build product recommendations built on top of association rules
   */
  def deserializeMultiUserRules(rules:String):MultiUserRules = read[MultiUserRules](rules)  

}

/**
 * Elements specify which data descriptions have to be pre-built
 * in an Elasticsearch index; these indexes are used to persist
 * trackable data and also mining results
 */
object Elements {
  
  val AMOUNT:String = "amount"
  
  val FEATURE:String = "feature"

  val ITEM:String = "item"
  
  val RULE:String = "rule"
    
  val SEQUENCE:String = "sequence"

  private val elements = List(AMOUNT,FEATURE,ITEM,RULE,SEQUENCE)  
  def isElement(element:String):Boolean = elements.contains(element)
  
}

object Messages extends BaseMessages

object Tasks {
  
  val CROSS_SELL:String = "cross_sell"
  val PLACEMENT:String  = "placement"
    
  val RECOMMENDATION:String = "recommendation"
  
  private val tasks = List(RECOMMENDATION)
  def isTask(task:String):Boolean = tasks.contains(task)
    
}

object Services {
  /*
   * Shopifyinsight. supports Association Analysis; the respective request
   * is delegated to Predictiveworks.
   */
  val ASSOCIATION:String = "association"
  /*
   * Shopifyinsight. supports Intent Recognition; the respective request is
   * delegated to Predictiveworks.
   */
  val INTENT:String = "intent"
  /*
   * Recommendation is an internal service of PIWIKinsight and uses ALS to
   * predict the most preferred item for a certain user
   */
  val RECOMMENDATION:String = "recommendation"
  /*
   * Shopifyinsight. supports Series Analysis; the respectiv request is 
   * delegated to Predictiveworks.
   */
  val SERIES:String = "series"
    
  private val services = List(ASSOCIATION,INTENT,SERIES)
  
  def isService(service:String):Boolean = services.contains(service)
  
}
