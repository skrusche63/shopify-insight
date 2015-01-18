package de.kp.shopify.elastic
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

class EsPPFBuilder {

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
                    
                /* site */
                .startObject(SITE_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
      
                /********** SEGMENT DATA **********/

                /* customer_supp */
                .startObject("customer_supp")
                  .field("type", "integer")
                .endObject()
               
                /* purchase_supp */
                .startObject("purchase_supp")
                  .field("type", "integer")
                .endObject()

                /* c_segment */
                .startObject("c_segment")
                  .field("type", "integer")
                .endObject()

                /* p_segment */
                .startObject("p_segment")
                  .field("type", "integer")
                .endObject()

                /* customer_type */
                .startObject("customer_type")
                  .field("type", "integer")
                .endObject()
                
              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }

}