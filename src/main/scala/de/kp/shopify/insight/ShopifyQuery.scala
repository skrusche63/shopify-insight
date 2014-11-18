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

import java.util.Date
import de.kp.shopify.insight.model.ShopifyStatus._

case class ShopifyQuery(
    
  var id:Long = -1,
  var sinceId:Long = -1,
  
  var fields:List[String] = List.empty[String],
  
  var fromCreateTimestamp:Date = new Date(),
  var toCreateTimestamp:Date = new Date(),
  
  var fromUpdateTimestamp:Date = new Date(),
  var toUpdateTimestamp:Date = new Date(),
  
  var fromProcessTimestamp:Date = new Date(),
  var toProcessTimestamp:Date = new Date(),
  
  var status:ShopifyStatus = ANY,
  var financialStatus:ShopifyStatus = ANY,
  
  var fulfillmentStatus:ShopifyStatus = ANY,
  
  var limit:Int = 50

)

class ShopifyQueryBuilder {

  private val item = new ShopifyQuery()
        
  def withFields(fields:List[String]):ShopifyQueryBuilder = {
    this.item.fields = fields
    this
  }

  def withId(id:Long):ShopifyQueryBuilder = {
    this.item.id = id
    this
  }

  def withSinceId(sinceId:Long):ShopifyQueryBuilder = {  
    this.item.sinceId = sinceId
    this
  }

  def withFromCreateTimestamp(fromCreateTimestamp:Date):ShopifyQueryBuilder = {
    this.item.fromCreateTimestamp = fromCreateTimestamp
    this
  }

  def withToCreateTimestamp(toCreateTimestamp:Date):ShopifyQueryBuilder = {
    this.item.toCreateTimestamp = toCreateTimestamp
    this
  }

  def withFromUpdateTimestamp(fromUpdateTimestamp:Date):ShopifyQueryBuilder = {
    this.item.fromUpdateTimestamp = fromUpdateTimestamp
    this
  }

  def withToUpdateTimestamp(toUpdateTimestamp:Date):ShopifyQueryBuilder = {
    this.item.toUpdateTimestamp = toUpdateTimestamp
    this
  }

  def withFromProcessTimestamp(fromProcessTimestamp:Date):ShopifyQueryBuilder = {
    this.item.fromProcessTimestamp = fromProcessTimestamp
    this
  }

  def withToProcessTimestamp(toProcessTimestamp:Date):ShopifyQueryBuilder = {
    this.item.toProcessTimestamp = toProcessTimestamp
    this
  }

  def withStatus(status:ShopifyStatus):ShopifyQueryBuilder = {
    this.item.status = status
    this
  }

  def withFinancialStatus(financialStatus:ShopifyStatus):ShopifyQueryBuilder = {
    this.item.financialStatus = financialStatus
    this
  }

  def withFulfillmentStatus(fulfillmentStatus:ShopifyStatus):ShopifyQueryBuilder = {
     this.item.fulfillmentStatus = fulfillmentStatus
     this
  }

  def withLimit(limit:Int):ShopifyQueryBuilder = {
    this.item.limit = limit
    this
  }

  def build():ShopifyQuery = this.item
  
}
