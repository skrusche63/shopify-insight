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

import org.joda.time.format.DateTimeFormat
import de.kp.spark.core.model._

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io.OrderBuilder

import scala.collection.mutable.HashMap

class ShopifyContext {

  private val (endpoint,apikey,password) = Configuration.shopify
  /*
   * The 'apikey' is used as the 'site' parameter when indexing
   * Shopify data with Elasticsearch
   */
  private val conf = new ShopifyConfiguration(endpoint,apikey,password)
  
  private val client = new ShopifyClient(conf)
  
  /**
   * This method is responsible for retrieving a set of orders representing
   * a certain time period; in order to e.g. fill a transaction darabase for
   * later data mining and predictive analytics, this method may be called
   * multiple times (e.g. with the help of a scheduler)
   */
  def getOrders(req:ServiceRequest):List[Order] = {
    
    val params = validateOrderParams(req.data)

    val orders = client.getOrders(params)    
    orders.map(order => new OrderBuilder().build(apikey,order))
    
  }
  
  def getProduct(pid:Long) = client.getProduct(pid)
  
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
  /**
   * A helper method to transform the request parameters into validated params
   */
  private def validateOrderParams(params:Map[String,String]):Map[String,String] = {

    val requestParams = HashMap.empty[String,String]
    
    if (params.contains("created_at_min")) {
      /*
       * Show orders created after date (format: 2008-12-31 03:00)
       */
      val time = params("created_at_min").toLong
      requestParams += "created_at_min" -> formatted(time)
      
    }
    
    if (params.contains("created_at_max")) {
      /*
       * Show orders created before date (format: 2008-12-31 03:00)
       */
      val time = params("created_at_max").toLong
      requestParams += "created_at_max" -> formatted(time)
      
    }
    
    if (params.contains("fields")) {
      /* Comma-separated list of fields to include in the response */
      val fields = params("fields")
      requestParams += "fields" -> fields
    
    }
    
    if (params.contains("financial_status")) {
      /*
       * authorized - Show only authorized orders
       * pending - Show only pending orders
       * paid - Show only paid orders
       * partially_paid - Show only partially paid orders
       * refunded - Show only refunded orders
       * voided - Show only voided orders
       * partially_refunded - Show only partially_refunded orders
       * any - Show all authorized, pending, and paid orders (default). This is a filter, not a value.
       * unpaid - Show all authorized, or partially_paid orders. This is a filter, not a value.
      */
      val status = params("financial_status")
      if (FinancialStatus.isStatus(status) == true) {
        requestParams += "financial_status" -> status
      
      } else {
        throw new ShopifyException("Wrong financial  status '" + status + "' provided.")
        
      }

    }
    
    if (params.contains("fulfillment_status")) {
      /*
       * shipped - Show orders that have been shipped
       * partial - Show partially shipped orders
       * unshipped - Show orders that have not yet been shipped
       * any - Show orders with any fulfillment_status. (default)
       */
      val status = params("fulfillment_status")
      if (FulfillmentStatus.isStatus(status) == true) {
        requestParams += "fulfillment_status" -> status
      
      } else {
        throw new ShopifyException("Wrong fulfillment  status '" + status + "' provided.")
        
      }
    
    }
    
    if (params.contains("limit")) {
      /* Amount of results: (default: 50) (maximum: 250) */   
      val limit = params("limit")
      requestParams += "page" -> limit
     
    }
    
    if (params.contains("page")) {
      /* Page to show: (default: 1) */      
      val page = params("page")
      requestParams += "page" -> page
      
    }

    if (params.contains("processed_at_min")) {
      /*
       * Show orders imported after date (format: 2008-12-31 03:00)
       */
      val time = params("processed_at_min").toLong
      requestParams += "processed_at_min" -> formatted(time)
      
    }

    if (params.contains("processed_at_max")) {
      /*
       * Show orders imported before date (format: 2008-12-31 03:00)
       */
      val time = params("processed_at_max").toLong
      requestParams += "processed_at_max" -> formatted(time)
      
    }
    
    if (params.contains("updated_at_min")) {
      /*
       * Show orders last updated after date (format: 2008-12-31 03:00)
       */
      val time = params("updated_at_min").toLong
      requestParams += "updated_at_min" -> formatted(time)
      
    }
    
    if (params.contains("updated_at_max")) {
      /*
       * Show orders last updated before date (format: 2008-12-31 03:00)
       */
      val time = params("updated_at_max").toLong
      requestParams += "updated_at_max" -> formatted(time)
      
    }
    
    if (params.contains("since_id")) {
      /*
       * Restrict results to after the specified ID
       */
      val since_id = params("since_id")
      requestParams += "since_id" -> since_id
      
    }

    if (params.contains("status")) {
      /*
       * open - All open orders (default)
       * closed - Show only closed orders
       * cancelled - Show only cancelled orders
       * any - Any order status
       * 
       */
      val status = params("status")
      if (Statuses.isStatus(status) == true) {
        requestParams += "status" -> status
      
      } else {
        throw new ShopifyException("Wrong status '" + status + "' provided.")
        
      }
      
    }
    
    requestParams.toMap
  
  }

}