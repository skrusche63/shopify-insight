package de.kp.shopify.insight.io
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.model._
import scala.collection.mutable.HashMap

class AssociationBuilder {
  
  def get(params:Map[String,String]):Map[String,String] = null

  def train(params:Map[String,String]):Map[String,String] = {
    
    /*
     * The subsequent set of parameters is mandatory for training
     * an association model and must be provided by the requestor
     */
    val com_mandatory = List(Names.REQ_SITE,Names.REQ_UID,Names.REQ_NAME,Names.REQ_SOURCE,Names.REQ_ALGORITHM)
    val data = HashMap.empty[String,String]
    
    for (field <- com_mandatory) data += field -> params(field)

    /*
     * The subsequent set of parameters is optional
     */
    val com_optional = List(Names.REQ_SINK)
    for (field <- com_optional) if (params.contains(field)) data += field -> params(field)

    /*
     * The following parameters depend on the source & sink selected
     */
    val source = data(Names.REQ_SOURCE)
    if (source == Sources.ELASTIC) {
      /*
       * Add index & mapping internally as the requestor does not
       * know the Elasticsearch index structure; the index MUST be
       * identical to that index, that has been created during the
       * 'collection' phase
       */
      data += Names.REQ_SOURCE_INDEX -> "orders"
      data += Names.REQ_SOURCE_TYPE  -> "items"
      
      data += Names.REQ_QUERY -> QueryBuilder.get(source,"item")
      
    } else {
      throw new Exception(String.format("""[UID: %s] The source '%s' is not supported.""",data(Names.REQ_UID),source))
    }
    
    if (data.contains(Names.REQ_SINK)) {
      
      val sink = data(Names.REQ_SINK)
      if (sink == Sinks.ELASTIC) {
        /*
         * Add index & mapping internally as the requestor does not
         * know the Elasticsearch index structure; the index MUST be
         * identical to that index, that has been created during the
         * 'collection' phase
         */
        data += Names.REQ_SINK_INDEX -> "orders"
        data += Names.REQ_SINK_TYPE  -> "rules"
        
      } else {
        throw new Exception(String.format("""[UID: %s] The sink '%s' is not supported.""",data(Names.REQ_UID),sink))
        
      }
      
    }
    
    /*
     * The subsequent parameters are model specific parameters
     */
    val mod_mandatory = List("k","minconf","weight")
    for (field <- mod_mandatory) data += field -> params(field)
    
    val mod_optional = List("delta")
    for (field <- mod_optional) if (params.contains(field)) data += field -> params(field)
   
    data.toMap
    
  }
  
}