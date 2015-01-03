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
case class InsightState(

  @JsonProperty("uid")
  uid:String,
  /*
   * timestamp of the order that forms the basis
   * for the derivation of the subsequent values
   */
  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("created_at_min")
  created_at_min:String,

  @JsonProperty("created_at_max")
  created_at_max:String,

  /*
   * today is the reference timestamp when this
   * analytics record has been built; it is used
   * to compute the recency and may also be used
   * to adapt the recency
   */
  @JsonProperty("today")
  today:Long,

  @JsonProperty("site")
  site:String,

  @JsonProperty("user")
  user:String,

  @JsonProperty("state")
  state:String,
  /*
   * The total number of orders within the timespan
   * defined by created_at_min and created_at_max
   */
  @JsonProperty("user_total")
  user_total:Int,

  @JsonProperty("user_clv_group")
  user_clv_group:String,

  @JsonProperty("user_total_spent")
  user_total_spent:Float,

  @JsonProperty("user_avg_amount")
  user_avg_amount:Float,

  @JsonProperty("user_max_amount")
  user_max_amount:Float,

  @JsonProperty("user_min_amount")
  user_min_amount:Float,

  @JsonProperty("user_diff_amount")
  user_diff_amount:Float,

  @JsonProperty("user_avg_timespan")
  user_avg_timespan:Long,

  @JsonProperty("user_max_timespan")
  user_max_timespan:Long,

  @JsonProperty("user_min_timespan")
  user_min_timespan:Long,

  @JsonProperty("user_timespan")
  user_timespan:Long,

  @JsonProperty("user_recency")
  user_recency:Long,

  @JsonProperty("user_day_pref")
  user_day_pref:List[DayPref],

  @JsonProperty("user_time_pref")
  user_time_pref:List[TimePref]

)