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

class STMHandler {

  def get(params:Map[String,String]):Map[String,String] = {
    // TODO
    null
  }

  def train(params:Map[String,String]):Map[String,String] = {
    
    /*
     * The subsequent set of parameters is mandatory for training
     * an association model and must be provided by the requestor
     */
    val com_mandatory = List(Names.REQ_SITE,Names.REQ_UID,Names.REQ_NAME)
    val data = HashMap.empty[String,String]
    
    for (field <- com_mandatory) data += field -> params(field)

    /*
     * The algorithm is restricted to 'MARKOV' and must be set
     * internally
     */
    data += Names.REQ_ALGORITHM -> "MARKOV"
    data += Names.REQ_INTENT    -> "STATE"
    
    data += Names.REQ_SOURCE -> "PARQUET"

    // TODO
    
    data.toMap
    
  }

}