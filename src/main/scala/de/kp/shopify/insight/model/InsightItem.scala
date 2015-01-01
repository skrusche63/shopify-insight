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

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

@JsonIgnoreProperties(ignoreUnknown = true)
case class InsightItem(

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("created_at_min")
  created_at_min:String,

  @JsonProperty("created_at_max")
  created_at_max:String,

  @JsonProperty("site")
  site:String,

  @JsonProperty("user")
  user:String,

  @JsonProperty("group")
  group:String,

  @JsonProperty("item")
  item:Int,

  @JsonProperty("score")
  score:Double,

  @JsonProperty("user_total")
  user_total:Int,

  @JsonProperty("item_quantity")
  item_quantity:Int,

  @JsonProperty("item_category")
  item_category:String,

  @JsonProperty("item_tags")
  item_tags:String

)