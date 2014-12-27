package de.kp.shopify.insight
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

import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.action.search.SearchResponse

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.query.QueryBuilder

class FindContext() {
  /*
   * Create an Elasticsearch node by interacting with
   * the Elasticsearch server on the local machine
   */
  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())

  def find(index:String,mapping:String,query:QueryBuilder):SearchResponse = {
    
    /*
     * Prepare search request: note, that we may have to introduce
     * a size restriction with .setSize method 
     */
    val response = client.prepareSearch(index).setTypes(mapping).setQuery(query)
                     .execute().actionGet()

    response
    
  }

}
