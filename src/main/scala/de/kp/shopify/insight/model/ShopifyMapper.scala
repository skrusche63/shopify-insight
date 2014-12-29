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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.model._

import org.joda.time.format.DateTimeFormat
import scala.collection.JavaConversions._

class ShopifyMapper {

  def extractCustomer(site:String,customer:ShopifyCustomer):Customer = {
    /*
     * The unique user identifier is retrieved from the
     * customer object and there from the 'id' field
     */
    val user = customer.id.toString
    /*
     * Retrieve the first & last name of a customer
     */
    val firstName = customer.first_name
    val lastName  = customer.last_name
    /*
     * Retrieve email data from customer
     */
    val emailAddress = customer.email
    val emailVerified = customer.verified_email
    /*
     * Determine marketing indicator and current state
     */
    val marketing = customer.accepts_marketing
    val state = customer.state
    /*
     * Determine order specific data
     */
    val lastOrder = customer.last_order_id.toString
    val ordersCount = customer.orders_count
    
    val totalSpent = customer.total_spent.toFloat
    
    Customer(site,user,firstName,lastName,emailAddress,emailVerified,marketing,state,lastOrder,ordersCount,totalSpent)
    
  }

  def extractProduct(site:String,product:ShopifyProduct):Product = {
    null
  }

  /**
   * A public method to extract those fields from a Shopify
   * order that describes an 'Order'
   */
  def extractOrder(site:String,order:ShopifyOrder):Order = {
    
    /*
     * The unique identifier of a certain order is used
     * for grouping all the respective items; the order
     * identifier is a 'Long' and must be converted into
     * a 'String' representation
     */
    val group = order.id.toString
    /*
     * The datetime this order was created:
     * "2014-11-03T13:51:38-05:00"
     */
    val created_at = order.created_at
    val timestamp = toTimestamp(created_at)
    /*
     * The unique user identifier is retrieved from the
     * customer object and there from the 'id' field
     */
    val user = order.customer.id.toString
    /*
     * The amount is retrieved from the total price
     */
    val amount = order.total_price.toFloat
    /*
     * Convert all line items of the respective order
     * into 'OrderItem' for indexing
     */
    val items = order.lineItems.map(lineItem => {
      /*
       * A shopify line item holds 3 different identifiers:
       * 
       * - 'id' specifies the unique identifier for this item,
       * 
       * - 'variant_id' specifies the product variant uniquely
       * 
       * - 'product_id' specifies the product uniquely
       * 
       * For further mining and prediction tasks, the 'product_id'
       * is used to uniquely identify a purchase item
       */
      val item = lineItem.product_id.toInt
      
      /*
       * In addition, we collect the following data from the line item
       */
      val name = lineItem.name
      val quantity = lineItem.quantity
      
      val currency = order.currency
      val price = lineItem.price
      
      val sku = lineItem.sku
      
      new OrderItem(item,name,quantity,currency,price,sku)
    
    })

    Order(site,user,timestamp,group,amount,items)
  
  }

  /**
   * A public method to extract those fields from a Shopify
   * order that describes an 'AmountObject'
   */
  def extractPurchase(site:String,order:ShopifyOrder):AmountObject = {
    
    /*
     * The datetime this order was created:
     * "2014-11-03T13:51:38-05:00"
     */
    val created_at = order.created_at
    val timestamp = toTimestamp(created_at)
    /*
     * The unique user identifier is retrieved from the
     * customer object and there from the 'id' field
     */
    val user = order.customer.id.toString
    /*
     * The amount is retrieved from the total price
     */
    val amount = order.total_price.toFloat

    AmountObject(site,user,timestamp,amount)
  
  }
   
  def toAmountTuple(order:Order):(String,String,Long,Float) = {
    (order.site,order.user,order.timestamp,order.amount)
  }
 
  def toAmountMap(order:Order):java.util.Map[String,Object] = {
        
    val data = new java.util.HashMap[String,Object]()
        
    data += Names.SITE_FIELD -> order.site
    data += "user" -> order.user
        
    data += "timestamp" -> order.timestamp.asInstanceOf[Object]
    data += "amount" -> order.amount.asInstanceOf[Object]

    data
    
  }
  
  def toItemMap(order:Order):List[java.util.Map[String,Object]] = {

    val items = order.items.map(_.item)
    items.map(item => {
    
      val data = new java.util.HashMap[String,Object]()
        
      data += Names.SITE_FIELD -> order.site
      data += "user" -> order.user
        
      data += "timestamp" -> order.timestamp.asInstanceOf[Object]
      data += "group" -> order.group

      data += Names.ITEM_FIELD -> item.asInstanceOf[Object]
      data += Names.SCORE_FIELD -> 0.0.asInstanceOf[Object]
      
      data
      
    })    
    
  }
 
  private def toTimestamp(text:String):Long = {
      
    //2014-11-03T13:51:38-05:00
    val pattern = "yyyy-MM-dd'T'HH:mm:ssZ"
    val formatter = DateTimeFormat.forPattern(pattern)
      
    val datetime = formatter.parseDateTime(text)
    datetime.toDate.getTime
    
  }  
}