package de.kp.shopify.insight.elastic
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class ESAggregateBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {
    
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              
              .startObject("properties")

                /* uid */
                .startObject(UID_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
                
                /* timestamp */
                .startObject(TIMESTAMP_FIELD)
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()

                /* created_at_min */
                .startObject("created_at_min")
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()

                /* created_at_max */
                .startObject("created_at_max")
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()

                /* total_orders */
                .startObject("total_orders")
                  .field("type", "integer")
                .endObject()

                /* total_amount */
                .startObject("total_amount")
                  .field("type", "double")
                .endObject()

                /* total_avg_amount */
                .startObject("total_avg_amount")
                  .field("type", "double")
                .endObject()

                /* total_max_amount */
                .startObject("total_max_amount")
                  .field("type", "double")
                .endObject()

                /* total_min_amount */
                .startObject("total_min_amount")
                  .field("type", "double")
                .endObject()
                
                /* total_stdev_amount */
                .startObject("total_stdev_amount")
                  .field("type", "double")
                .endObject()

                /* total_variance_amount */
                .startObject("total_variance_amount")
                  .field("type", "double")
                .endObject()

                /* total_avg_timespan */
                .startObject("total_avg_timespan")
                  .field("type", "double")
                .endObject()

                /* total_max_timespan */
                .startObject("total_max_timespan")
                  .field("type", "double")
                .endObject()

                /* total_min_timespan */
                .startObject("total_min_timespan")
                  .field("type", "double")
                .endObject()
                
                /* total_stdev_timespan */
                .startObject("total_stdev_timespan")
                  .field("type", "double")
                .endObject()

                /* total_variance_timespan */
                .startObject("total_variance_timespan")
                  .field("type", "double")
                .endObject()
                
                /* total_day_supp */
                .startObject("total_day_supp")
                  .startObject("properties")

                    .startObject("day")
                      .field("type","integer")
                    .endObject

                    .startObject("supp")
                      .field("type","integer")
                    .endObject
                    
                  .endObject()    
                .endObject()
                
                /* total_day_pref */
                .startObject("total_day_pref")
                  .startObject("properties")

                    .startObject("day")
                      .field("type","integer")
                    .endObject

                    .startObject("score")
                      .field("type","double")
                    .endObject
                    
                  .endObject()    
                .endObject()

                /* total_time_supp */
                .startObject("total_time_supp")
                  .startObject("properties")

                    .startObject("time")
                      .field("type","integer")
                    .endObject

                    .startObject("supp")
                      .field("type","integer")
                    .endObject
                    
                  .endObject()
                .endObject()

                /* total_time_pref */
                .startObject("total_time_pref")
                  .startObject("properties")

                    .startObject("time")
                      .field("type","integer")
                    .endObject

                    .startObject("score")
                      .field("type","double")
                    .endObject
                    
                  .endObject()
                .endObject()

                /* total_item_supp */
                .startObject("total_item_supp")
                  .startObject("properties")

                    .startObject("item")
                      .field("type","integer")
                    .endObject

                    .startObject("supp")
                      .field("type","integer")
                    .endObject
                    
                  .endObject()
                .endObject()

                /* total_item_pref */
                .startObject("total_item_pref")
                  .startObject("properties")

                    .startObject("item")
                      .field("type","integer")
                    .endObject

                    .startObject("score")
                      .field("type","double")
                    .endObject
                    
                  .endObject()
                .endObject()
                 
              .endObject()
              
            .endObject()
          .endObject()
          
    builder
  
  }

}