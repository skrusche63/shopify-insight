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

class ESStateBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * We build a denormalized description of the monetary and temporal
     * dimension of the customers' purchase transactions for a certain
     * period of time (created_at_min -> created_at_max).
     * 
     * Automatic identifier creation is required here as a customer holds
     * multiple records and cannot be used for identifying the documents
     */
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              
              .startObject("properties")
	            /*
	             * The subsequent fields are shared with Predictiveworks'
	             * Intent Recognition engine and must also be described 
	             * by a field or metadata specification
	             */
                    
                /* site */
                .startObject(SITE_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()

                /* user */
                .startObject(USER_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()

                /* timestamp */
                .startObject(TIMESTAMP_FIELD)
                  .field("type", "long")
                .endObject()

                /* state */
                .startObject(STATE_FIELD)
                  .field("type", "string")
                .endObject()

	            /*
	             * The subsequent fields are used for evaluation within
	             * the Shopify insight server
	             */

                /* uid */
                .startObject(UID_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()

                /* created_at_min */
                .startObject("created_at_min")
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()

                /* created_at_max */
                .startObject("created_at_max")
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
 
                /* today */
                .startObject("today")
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()
                
                /*
	             * Denormalized description of the RFM data retrieved
	             * from all user transactions taken into account, i.e. 
	             * from the 30, 60 or 90 days
	             */

                /* user_clv_group */
                .startObject("user_clv_group")
                  .field("type", "string")
                .endObject()

                /* user_total */
                .startObject("user_total")
                  .field("type", "integer")
                .endObject()

                /* user_total_spent */
                .startObject("user_total_spent")
                  .field("type", "float")
                .endObject()

                /* user_avg_amount */
                .startObject("user_avg_amount")
                  .field("type", "float")
                .endObject()

                /* user_max_amount */
                .startObject("user_max_amount")
                  .field("type", "float")
                .endObject()

                /* user_min_amount */
                .startObject("user_min_amount")
                  .field("type", "float")
                .endObject()

                /* user_diff_amount */
                .startObject("user_diff_amount")
                  .field("type", "float")
                .endObject()

                /* user_avg_timespan */
                .startObject("user_avg_timespan")
                  .field("type", "long")
                .endObject()

                /* user_max_timespan */
                .startObject("user_max_timespan")
                  .field("type", "long")
                .endObject()

                /* user_min_timespan */
                .startObject("user_min_timespan")
                  .field("type", "long")
                .endObject()

                /* user_timespan */
                .startObject("user_timespan")
                  .field("type", "long")
                .endObject()

                /* user_recency */
                .startObject("user_recency")
                  .field("type", "long")
                .endObject()
                
                /* user_day_pref */
                .startObject("user_day_pref")
                  .startObject("properties")

                    .startObject("day")
                      .field("type","integer")
                    .endObject

                    .startObject("score")
                      .field("type","double")
                    .endObject
                    
                  .endObject()    
                .endObject()

                /* user_time_pref */
                .startObject("user_time_pref")
                  .startObject("properties")

                    .startObject("time")
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