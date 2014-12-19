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

class PurchaseBuilder {

  def get(params:Map[String,String]):Map[String,String] = {
    null
  }

  def train(params:Map[String,String]):Map[String,String] = {
    
    /*
     * The subsequent set of parameters is mandatory for training
     * an association model and must be provided by the requestor
     */
    val com_mandatory = List(Names.REQ_SITE,Names.REQ_UID,Names.REQ_NAME,Names.REQ_SOURCE)
    val data = HashMap.empty[String,String]
    
    for (field <- com_mandatory) data += field -> params(field)

    /*
     * The algorithm is restricted to 'MARKOV' and must be set
     * internally
     */
    data += Names.REQ_ALGORITHM -> "MARKOV"
    data += Names.REQ_INTENT    -> "STATE"
    
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
      data += Names.REQ_SOURCE_TYPE  -> "states"
      
      data += Names.REQ_QUERY -> QueryBuilder.get(source,"state")
      
    } else {
      throw new Exception(String.format("""[UID: %s] The source '%s' is not supported.""",data(Names.REQ_UID),source))
    }
   
    data.toMap
    
  }

}