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

case class ShopifyQuery(
    
  var id:Long = -1,
  var since_id:Long = -1,
  
  var fields:List[String] = List.empty[String],
  
  var fromCreateTimestamp:String = "",
  var toCreateTimestamp:String = "",
  
  var fromUpdateTimestamp:String = "",
  var toUpdateTimestamp:String = "",
  
  var fromProcessTimestamp:String = "",
  var toProcessTimestamp:String = "",
  
  var status:String = "any",
  var financialStatus:String = "any",
  
  var fulfillmentStatus:String = "any",
  
  var limit:Int = 50,
  var page:Int = 1

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
    this.item.since_id = sinceId
    this
  }

  def withFromCreateTimestamp(fromCreateTimestamp:String):ShopifyQueryBuilder = {
    this.item.fromCreateTimestamp = fromCreateTimestamp
    this
  }

  def withToCreateTimestamp(toCreateTimestamp:String):ShopifyQueryBuilder = {
    this.item.toCreateTimestamp = toCreateTimestamp
    this
  }

  def withFromUpdateTimestamp(fromUpdateTimestamp:String):ShopifyQueryBuilder = {
    this.item.fromUpdateTimestamp = fromUpdateTimestamp
    this
  }

  def withToUpdateTimestamp(toUpdateTimestamp:String):ShopifyQueryBuilder = {
    this.item.toUpdateTimestamp = toUpdateTimestamp
    this
  }

  def withFromProcessTimestamp(fromProcessTimestamp:String):ShopifyQueryBuilder = {
    this.item.fromProcessTimestamp = fromProcessTimestamp
    this
  }

  def withToProcessTimestamp(toProcessTimestamp:String):ShopifyQueryBuilder = {
    this.item.toProcessTimestamp = toProcessTimestamp
    this
  }

  def withStatus(status:String):ShopifyQueryBuilder = {
    this.item.status = status
    this
  }

  def withFinancialStatus(financialStatus:String):ShopifyQueryBuilder = {
    this.item.financialStatus = financialStatus
    this
  }

  def withFulfillmentStatus(fulfillmentStatus:String):ShopifyQueryBuilder = {
     this.item.fulfillmentStatus = fulfillmentStatus
     this
  }

  def withLimit(limit:Int):ShopifyQueryBuilder = {
    this.item.limit = limit
    this
  }

  def withPage(page:Int):ShopifyQueryBuilder = {
    this.item.page = page
    this
  }

  def build():ShopifyQuery = this.item
  
}
