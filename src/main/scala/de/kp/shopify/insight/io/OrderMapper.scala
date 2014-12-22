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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.model._

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.collection.JavaConversions._

class OrderMapper {
   
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
  
}