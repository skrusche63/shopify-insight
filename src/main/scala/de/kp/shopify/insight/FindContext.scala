package de.kp.shopify.insight

import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchResponse

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.xcontent.{XContentFactory}

import org.elasticsearch.index.query.QueryBuilders

import org.elasticsearch.client.Requests
import scala.collection.JavaConversions._

class FindContext() {
  /*
   * Create an Elasticsearch node by interacting with
   * the Elasticsearch server on the local machine
   */
  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())

  def find(index:String,mapping:String,query:String):SearchResponse = {
    
    val searchQuery = QueryBuilders.queryString(query)
    /*
     * Prepare search request: note, that we may have to introduce
     * a size restriction with .setSize method 
     */
    val response = client.prepareSearch(index).setTypes(mapping).setQuery(searchQuery)
                     .execute().actionGet()

    response
    
  }

}
