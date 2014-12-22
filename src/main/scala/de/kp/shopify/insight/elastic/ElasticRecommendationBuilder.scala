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

class ElasticRecommendationBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'
     */
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
               .startObject("properties")
                    
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

               /* uid */
               .startObject(UID_FIELD)
                 .field("type", "string")
                 .field("index", "not_analyzed")
               .endObject()
                    
               /* recommendation */
               .startObject(RECOMMENDATION_FIELD)
                 .field("type", "string")
                 .field("index", "not_analyzed")
               .endObject()

               /* consequent */
               .startObject(CONSEQUENT_FIELD)
                 .field("type", "integer")
               .endObject()//

               /* support */
               .startObject(SUPPORT_FIELD)
                 .field("type", "integer")
               .endObject()

               /* confidence */
               .startObject(CONFIDENCE_FIELD)
                 .field("type", "double")
               .endObject()
                          
               /* weight */
               .startObject(WEIGHT_FIELD)
                 .field("type", "double")
               .endObject()

               .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }
 
}