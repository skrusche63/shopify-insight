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

class ElasticForecastBuilder {

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
               
               /* step */
               .startObject(STEP_FIELD)
                  .field("type", "integer")
               .endObject()//

               /* amount */
               .startObject(AMOUNT_FIELD)
                  .field("type", "float")
               .endObject()

               /* timestamp */
               .startObject(TIMESTAMP_FIELD)
                 .field("type", "long")
               .endObject()
               
               /* score */
               .startObject(SCORE_FIELD)
                 .field("type", "double")
               .endObject()

               .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }
  
  def createSource(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val source = HashMap.empty[String,String]
    
    source += Names.SITE_FIELD -> params(Names.SITE_FIELD)
    source += Names.USER_FIELD -> params(Names.USER_FIELD)
      
    source += Names.STEP_FIELD -> params(Names.STEP_FIELD)
 
    source += Names.AMOUNT_FIELD -> params(Names.AMOUNT_FIELD)
    source += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD)
      
    source += Names.SCORE_FIELD -> params(Names.SCORE_FIELD)

    source
    
  }

}