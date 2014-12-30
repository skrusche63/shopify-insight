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

class ESItemBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {
    
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              
              .startObject("properties")
	            /*
	             * The subsequent fields are shared with Predictiveworks'
	             * Association Analysis engine and must also be described 
	             * by a field or metadata specification
	             */

                /* uid */
                .startObject(UID_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
                    
                /* site */
                .startObject(SITE_FIELD)
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()

                /* user */
                .startObject(USER_FIELD)
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()//

                /* timestamp */
                .startObject(TIMESTAMP_FIELD)
                  .field("type", "long")
                .endObject()

                /* group */
                .startObject(GROUP_FIELD)
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()//

                /* item */
                .startObject(ITEM_FIELD)
                   .field("type", "integer")
                .endObject()

                /* score */
                .startObject(SCORE_FIELD)
                   .field("type", "double")
                .endObject()

                /*
	             * The subsequent fields are used for evaluation within
	             * the Shopify insight server
	             */

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
                
                /*
	             * Denormalized description of the ITEM data retrieved
	             * from all transactions taken into account, i.e. from 
	             * the 30, 60 or 90 days
	             */

                /* total_orders */
                .startObject("total_orders")
                  .field("type", "integer")
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

                /* user_total */
                .startObject("user_total")
                  .field("type", "integer")
                .endObject()

                /* item_quantity */
                .startObject("item_quantity")
                  .field("type", "integer")
                .endObject()
              
              .endObject()
              
            .endObject()
          .endObject()
    
    builder
  
  }

}