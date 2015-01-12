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

class ESLocationBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {

    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              .startObject("properties")
      
                /********** METADATA **********/

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
                    
                /* site */
                .startObject(SITE_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
      
                /********** USER DATA **********/

                /* user */
                .startObject(USER_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
               
                /* ip_address */
                .startObject("ip_address")
                  .field("type", "string")
                .endObject()//

                /* time */
                .startObject("time")
                  .field("type", "long")
                .endObject()

                /* countryname */
                .startObject("countryname")
                  .field("type", "string")
                .endObject()

                /* countrycode */
                .startObject("countrycode")
                  .field("type", "string")
                .endObject()

                /* region */
                .startObject("region")
                  .field("type", "string")
                .endObject()

                /* regionname */
                .startObject("regionname")
                  .field("type", "string")
                .endObject()

                /* areacode */
                .startObject("areacode")
                  .field("type", "integer")
                .endObject()

                /* dmacode */
                .startObject("dmacode")
                  .field("type", "integer")
                .endObject()

                /* metrocode */
                .startObject("metrocode")
                  .field("type", "integer")
                .endObject()

                /* city */
                .startObject("city")
                  .field("type", "string")
                .endObject()

                /* postalcode */
                .startObject("postalcode")
                  .field("type", "string")
                .endObject()

                /* location */
                .startObject("location")
                  .field("type", "geo_point")
                .endObject()
                
              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }

}