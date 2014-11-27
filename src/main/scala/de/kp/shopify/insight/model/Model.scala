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

/**
 * OrderItem is used to describe a single order or purchase
 * related entity that is indexed in an Elasticsearch index 
 * for later mining and prediction tasks
 */
case class OrderItem(
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

case class Order(items:List[OrderItem])
case class Orders(items:List[Order])

/**
 * A placement is a list of products that refers
 * to the consequent part of an association rule
 */
case class Placement(products:List[ShopifyProduct])

case class Recommendation(
  /* A recommendation is described on a per user basis */
  site:String,user:String,products:List[ShopifyProduct]
)

case class Recommendations(uid:String,items:List[Recommendation])

case class Rule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double)

case class Rules(items:List[Rule])

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
  def deserializeResponse(response:String):ServiceResponse = read[ServiceResponse](response)
  
  /** 
   * Multi user rules specify the result of association analysis and are used to 
   * build product recommendations built on top of association rules
   */
  def deserializeMultiUserRules(rules:String):MultiUserRules = read[MultiUserRules](rules)  
  /**
   * Rules are the result of either association or series analysis; rules e.g. used to
   * answer questions about product placement
   */
  def deserializeRules(rules:String):Rules = read[Rules](rules)

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

object Messages {

  def TASK_IS_UNKNOWN(uid:String,task:String):String = 
    String.format("""[UID: %s] The task '%s' is unknown.""", uid, task)

    def TRACKING_DATA_RECEIVED(uid:String):String = 
    String.format("""[UID: %s] Shopify data received and tracking initiated.""", uid)
  
}

/**
 * Metadata specifies the information topics that are actually supported
 * when registering metadata (or field description) for a certain predictive
 * engine
 */
object Metadata {
  
  val FEATURE:String = "feature"
  
  val FIELD:String = "field"

  val LOYALTY:String = "loyalty"
    
  val PURCHASE:String = "purchase"
    
  val SEQUENCE:String = "sequence"

  private val fields = List(FEATURE,FIELD,LOYALTY,PURCHASE,SEQUENCE)  
  def isMetadata(field:String):Boolean = fields.contains(field)
  
}

object Tasks {
  
  val PLACEMENT:String = "placement"
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

/**
 * The TaskMapper decouples external task descriptions from internal ones
 */
object TaskMapper {

  def get(service:String,task:String):String = {
    
    service match {
    
      case Services.ASSOCIATION => {
        
        task match {
          /*
           * A product placement task is mapped onto
           * the internal 'antecedent' task
           */
          case Tasks.PLACEMENT => "antecedent"
          /*
           * All other task are actually not mapped onto
           * internal representation
           */          
          case _ => task
        
        }
      
      }
      /*
       * All other task are actually not mapped onto
       * internal representation
       */
      case _ => task
    }
    
  }

}