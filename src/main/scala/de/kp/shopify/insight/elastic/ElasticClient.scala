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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.io._

import de.kp.spark.core.spec.FieldBuilder
import de.kp.spark.core.redis.RedisCache

import de.kp.shopify.insight.Configuration

import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.action.search.SearchResponse

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.query.QueryBuilder

import java.util.concurrent.locks.ReentrantLock

class ElasticClient {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)
  /*
   * Create an Elasticsearch node by interacting with
   * the Elasticsearch server on the local machine
   */
  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())
  private val indexCreationLock = new ReentrantLock()  
  
  private def create(index:String,mapping:String,builder:XContentBuilder) {
        
    try {
      
      indexCreationLock.lock()
      val indices = client.admin().indices
      /*
       * Check whether referenced index exists; if index does not
       * exist, create one
       */
      val existsRes = indices.prepareExists(index).execute().actionGet()            
      if (existsRes.isExists() == false) {
        
        val createRes = indices.prepareCreate(index).execute().actionGet()            
        if (createRes.isAcknowledged() == false) {
          new Exception("Failed to create " + index)
        }
            
      }

      /*
       * Check whether the referenced mapping exists; if mapping
       * does not exist, create one
       */
      val prepareRes = indices.prepareGetMappings(index).setTypes(mapping).execute().actionGet()
      if (prepareRes.mappings().isEmpty) {

        val mappingRes = indices.preparePutMapping(index).setType(mapping).setSource(builder).execute().actionGet()
            
        if (mappingRes.isAcknowledged() == false) {
          new Exception("Failed to create mapping for " + index + "/" + mapping)
        }            

      }

    } catch {
      case e:Exception => {
        logger.error(e.getMessage())

      }
       
    } finally {
     indexCreationLock.unlock()
    }
    
  }

  def close() {
    if (node != null) node.close()
  }
    
  def createIndex(params:Map[String,String],index:String,mapping:String,topic:String):Boolean = {
    
    try {
      
      /**********************************************************************
       * 
       *                     SUB PROCESS 'COLLECT'
       *                     
       *********************************************************************/
      
      if (topic == "customer") {

        val builder = new ESCustomerBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "order") {

        val builder = new ESOrderBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "product") {

        val builder = new ESProductBuilder().createBuilder(mapping)
        create(index,mapping,builder)
     
      } 
      
      /**********************************************************************
       * 
       *                     SUB PROCESS 'LOAD'
       *                     
       *********************************************************************/

      else if (topic == "CCN") {

        val builder = new EsCCNBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "LOC") {

        val builder = new EsLOCBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "POM") {

        val builder = new EsPOMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "PPF") {

        val builder = new EsPPFBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "PRM") {

        val builder = new EsPRMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "RFM") {

        val builder = new EsRFMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "UFM") {

        val builder = new EsUFMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
        
      } else if (topic == "loyalty") {
        /*
         * Topic 'loyalty' is indexed by the loyalty modeler and does not require 
         * a metadata specification as these data are not shared with predictive
         * engines
         */
        val builder = new ESLoyaltyBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "profile") {
        /*
         * Topic 'profile' is indexed by user profiler and does not require a
         * metadata specification as these data are not shared with predictive
         * engines
         */
        val builder = new ElasticProfileBuilder().createBuilder(mapping)
        create(index,mapping,builder)
          
      } else if (topic == "recommendation") {
        /*
         * Topic 'recommendation' is indexed by the loyalty modeler and does not 
         * require a metadata specification as these data are not shared with 
         * predictive engines
         */
        val builder = new ESRecommendationBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "state") {
        /*
         * Topic 'state' is indexed by the collector actor and the respective
         * index must by shared with Predictiveworks' Intent Recognition engine;
         * due to this fact, we also have to build metadata specifications to 
         * enable this engine to access the Elasticsearch index
         */
        val builder = new ESStateBuilder().createBuilder(mapping)    
        create(index,mapping,builder)
        /*
         * Topics 'item' and 'state' are prepared by the collector and used
         * by the Association Analysis and Intent Recognition engine; due to
         * this fact, we also have to build metadata specifications to enable
         * these engines to access the Elasticsearch indexes 
         */      
        val fields = new FieldBuilder().build(params,topic)
      
        /*
         * The name of the model to which these fields refer cannot be provided
         * by the user; we therefore have to re-pack the service request to set
         * the name of the model
         */
        val excludes = List(Names.REQ_NAME)
        val data = Map(Names.REQ_NAME -> mapping) ++  params.filter(kv => excludes.contains(kv._1) == false)  
     
        if (fields.isEmpty == false) cache.addFields(data, fields.toList)
       
      } else if (topic == "task") {

        val builder = new ESTaskBuilder().createBuilder(mapping)
        create(index,mapping,builder)
           
      }
      
      true
    
    } catch {
      case e:Exception => false

    }
    
  }
  
  def putSource(index:String,mapping:String,source:XContentBuilder):Boolean = {
     
    try {
    
      val writer = new ElasticWriter()
        
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        /*
         * Writing the sources to the respective index throws an
         * exception in case of an error; note, that the writer is
         * automatically closed 
         */
        writer.writeJSON(index, mapping, source)      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }
    
  def putSources(index:String,mapping:String,sources:List[XContentBuilder]):Boolean = {
     
    try {
    
      val writer = new ElasticWriter()
        
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        /*
         * Writing the sources to the respective index throws an
         * exception in case of an error; note, that the writer is
         * automatically closed 
         */
        writer.writeBulkJSON(index, mapping, sources)      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }

  def count(index:String,mapping:String,query:QueryBuilder):Int = {
    
    val response = client.prepareCount(index).setTypes(mapping).setQuery(query)
                     .execute().actionGet()
    /*
     * We restrict the count to an integer, as we use the result 
     * as the predefined size of a search request
     */                 
    response.getCount().toInt

  }
  
  def find(index:String,mapping:String,query:QueryBuilder):SearchResponse = {
    
    /*
     * Prepare search request: note, that we may have to introduce
     * a size restriction with .setSize method 
     */
    val response = client.prepareSearch(index).setTypes(mapping).setQuery(query)
                     .execute().actionGet()

    response
    
  }

  def find(index:String,mapping:String,query:QueryBuilder,size:Int):SearchResponse = {
    
    /*
     * Prepare search request: note, that we may have to introduce
     * a size restriction with .setSize method 
     */
    val response = client.prepareSearch(index).setTypes(mapping).setQuery(query).setSize(size)
                     .execute().actionGet()

    response
    
  }

  def getAsMap(index:String,mapping:String,id:String):java.util.Map[String,Object] = {
    
    val response = client.prepareGet(index,mapping,id).execute().actionGet()
    if (response.isExists()) response.getSource else null
    
  }

  def getAsString(index:String,mapping:String,id:String):String = {
    
    val response = client.prepareGet(index,mapping,id).execute().actionGet()
    if (response.isExists()) response.getSourceAsString else null
    
  }

}