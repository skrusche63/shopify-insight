package de.kp.shopify.elastic.plugin
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

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsCPR(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** USER DATA **********/

  @JsonProperty("user")
  user:String,

  @JsonProperty("item")
  item:Int,

  @JsonProperty("score")
  score:Double,

  @JsonProperty("customer_type")
  customer_type:Int
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsCPRList(entries:List[EsCPR])

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsPRM(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** RULE DATA **********/

  @JsonProperty("antecedent")
  antecedent:Seq[Int],

  @JsonProperty("consequent")
  consequent:Seq[Int],

  @JsonProperty("support")
  support:Int,

  @JsonProperty("total")
  total:Int,

  @JsonProperty("confidence")
  confidence:Double,

  @JsonProperty("customer_type")
  customer_type:Int  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsBPR(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,
                
  /********** PRODUCT DATA **********/
  
  @JsonProperty("items")
  items:Seq[Int],
  
  @JsonProperty("score")
  score:Double,

  @JsonProperty("customer_type")
  customer_type:Int
  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsBPRList(entries:List[EsBPR])
