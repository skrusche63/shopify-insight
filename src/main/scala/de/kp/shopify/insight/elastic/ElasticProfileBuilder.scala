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

class ElasticProfileBuilder {

  import de.kp.spark.core.Names._

  /**
   * The user profile is created from the association rule model,
   * state transition model, and hidden state model and leverages
   * these models to infer predictions with respect to the last
   * purchase transaction made by the respective user
   */
  def createBuilder(mapping:String):XContentBuilder = {

    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              .startObject("properties")
              
                /********** COMMON          **********/ 
                
                /* uid */
                .startObject(UID_FIELD)
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()//
                    
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
              
                /*
                 * The 'forecast' part of the user profile specifies sale predictions
                 * n steps ahead, where 'n' is usually set to '3'
                 */ 
                .startObject("forecast")
                  .startObject("properties")

                    .startObject("step")
                      .field("type","integer")
                    .endObject

                    .startObject("state")
                      .field("type","string")
                    .endObject

                    .startObject("amount")
                      .field("type","float")
                    .endObject

                    .startObject("days")
                      .field("type","float")
                    .endObject

                    .startObject("score")
                      .field("type","double")
                    .endObject
                  
                  .endObject()
                .endObject()
                
                /*
                 * The 'loyalty' part of the user profile describes the 
                 * trajectory of loyalty states for visualization purpose.
                 * 
                 * The more relevant information, that can also be used for
                 * segmentation comes from the state percentage description
                 */
                .startObject("loyalty")
                  .startObject("properties")

                    .startObject("low")
                      .field("type","double")
                    .endObject
                
                    .startObject("normal")
                      .field("type","double")
                    .endObject
                
                    .startObject("high")
                      .field("type","double")
                    .endObject

                    .startObject("trajectory")
                      .field("type","string")
                    .endObject
                  
                  .endObject()
                .endObject()

                /*
                 * The 'recommendation' part of the user profile specifies
                 * the best rule based list of products, that have not been
                 * purchased (with respect to the last transaction)                
                 */                 
                .startObject("recommendation")
                  .startObject("properties")
                  
                    .startObject("support")
                      .field("type","integer")
                    .endObject()

                    .startObject("total")
                      .field("type","integer")
                    .endObject()
 
                    .startObject("confidence")
                      .field("type","double")
                    .endObject()
  
                    .startObject("weight")
                      .field("type","double")
                    .endObject()
  
                    .startObject("products")
                      .field("type","string")
                    .endObject()

                  .endObject()
                .endObject()
      
              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder
    
  }

}