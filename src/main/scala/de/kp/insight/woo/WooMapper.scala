package de.kp.insight.woo
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

import de.kp.insight._
import de.kp.insight.model._

class WooMapper(ctx:RequestContext) {

  /**
   * A public method to extract those fields from a WooCommerce
   * customer that describes a 'Customer'
   */
  def extractCustomer(site:String,customer:WooCustomer):Customer = {
    null    
  }

  /**
   * A public method to extract those fields from a WooCommerce
   * order that describes an 'Order'
   */
  def extractOrder(site:String,order:WooOrder):Order = {
    null    
  }
  /**
   * A public method to extract those fields from a WooCommerce
   * product that describes a 'Product'
   */
  def extractProduct(site:String,product:WooProduct):Product = {
    null    
  }

}