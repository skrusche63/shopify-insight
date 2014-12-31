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

import de.kp.spark.core.Names

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ESLoyaltyBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'
     */
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
               .startObject("properties")

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
               
               /* trajectory */
               .startObject(TRAJECTORY_FIELD)
                  .field("type", "string")
               .endObject()
               
               /* low */
               .startObject("low")
                  .field("type", "double")
               .endObject()
               
               /* norm */
               .startObject("norm")
                  .field("type", "double")
               .endObject()
               
               /* high */
               .startObject("high")
                  .field("type", "double")
               .endObject()

               .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }

}