package de.kp.shopify.insight.io
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

import de.kp.spark.core.model._

import de.kp.shopify.insight.model._
import scala.collection.mutable.{ArrayBuffer,HashMap}
/**
 * The RequestBuilder is responsible for building tracking request
 * based on a specific predictive engine and respective shopify data
 */
class RequestBuilder {

  def build(order:Order,topic:String):ServiceRequest = {
    
    topic match {
      
      case "amount" => buildAmountRequest(order)      
      case "item"   => buildItemRequest(order)
      
      case _ => null
    
    }
    
  }
  
  private def buildAmountRequest(order:Order):ServiceRequest = {
        
    val data = HashMap.empty[String,String]
        
    data += "site" -> order.site
    data += "user" -> order.user
        
    data += "timestamp" -> order.timestamp.toString
    data += "amount" -> order.amount.toString

    new ServiceRequest("","",data.toMap)

  }
  
  private def buildItemRequest(order:Order):ServiceRequest = {
        
    val data = HashMap.empty[String,String]
        
    data += "site" -> order.site
    data += "user" -> order.user
        
    data += "timestamp" -> order.timestamp.toString
    data += "group" -> order.group
        
    val items = ArrayBuffer.empty[Int]
    for (record <- order.items) {
      items += record.item
    }
        
    data += "item" -> items.mkString(",")
    new ServiceRequest("","",data.toMap)
    
  }
  
}