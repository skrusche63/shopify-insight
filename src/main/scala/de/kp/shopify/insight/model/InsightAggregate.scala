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
case class DaySupp(

  @JsonProperty("day")
  day:Int,

  @JsonProperty("supp")
  supp:Int
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class DayPref(

  @JsonProperty("day")
  day:Int,

  @JsonProperty("score")
  score:Double
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class TimeSupp(

  @JsonProperty("time")
  time:Int,

  @JsonProperty("supp")
  supp:Int
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class TimePref(

  @JsonProperty("time")
  time:Int,

  @JsonProperty("score")
  score:Double
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class ItemSupp(

  @JsonProperty("item")
  item:Int,

  @JsonProperty("supp")
  supp:Int
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class ItemPref(

  @JsonProperty("item")
  item:Int,

  @JsonProperty("score")
  score:Double
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class InsightAggregate(

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("created_at_min")
  created_at_min:String,

  @JsonProperty("created_at_max")
  created_at_max:String,

  @JsonProperty("total_orders")
  total_orders:Int,

  @JsonProperty("total_amount")
  total_amount:Float,

  @JsonProperty("total_avg_amount")
  total_avg_amount:Float,

  @JsonProperty("total_max_amount")
  total_max_amount:Float,

  @JsonProperty("total_min_amount")
  total_min_amount:Float,

  @JsonProperty("total_avg_timespan")
  total_avg_timespan:Long,

  @JsonProperty("total_max_timespan")
  total_max_timespan:Long,

  @JsonProperty("total_min_timespan")
  total_min_timespan:Long,

  @JsonProperty("total_day_supp")
  total_day_supp:List[DaySupp],

  @JsonProperty("total_day_pref")
  total_day_pref:List[DayPref],

  @JsonProperty("total_time_supp")
  total_time_supp:List[TimeSupp],

  @JsonProperty("total_time_pref")
  total_time_pref:List[TimePref],

  @JsonProperty("total_item_supp")
  total_item_supp:List[ItemSupp],

  @JsonProperty("total_item_pref")
  total_item_pref:List[ItemPref]
    
)