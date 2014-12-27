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
import de.kp.spark.core.elastic._

import de.kp.spark.core.spec.FieldBuilder
import de.kp.spark.core.redis.RedisCache

import de.kp.shopify.insight.Configuration

class ElasticHandler {

  private val (host,port) = Configuration.redis
  protected val cache = new RedisCache(host,port.toInt)
    
  def createIndex(params:Map[String,String],index:String,mapping:String,topic:String):Boolean = {
    
    try {
      
      if (topic == "item" || topic == "state") {
        
        val builder = ElasticBuilderFactory.getBuilder(topic,mapping,List.empty[String],List.empty[String])
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
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
     
      } else if (topic == "forecast") {
        /*
         * Topic 'forecast' is indexed by the forecast modeler and does not require 
         * a metadata specification as these data are not shared with predictive
         * engines
         */
        val builder = new ElasticForecastBuilder().createBuilder(mapping)
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
        
      } else if (topic == "loyalty") {
        /*
         * Topic 'loyalty' is indexed by the loyalty modeler and does not require 
         * a metadata specification as these data are not shared with predictive
         * engines
         */
        val builder = new ElasticLoyaltyBuilder().createBuilder(mapping)
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
          
      } else if (topic == "recommendation") {
        /*
         * Topic 'recommendation' is indexed by the loyalty modeler and does not 
         * require a metadata specification as these data are not shared with 
         * predictive engines
         */
        val builder = new ElasticRecommendationBuilder().createBuilder(mapping)
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
       
      } else if (topic == "rule") {
        /*
         * Topic 'rule' is indexed by relation modeler and does not require a
         * metadata specification as these data are not shared with predictive
         * engines
         */
        val builder = ElasticBuilderFactory.getBuilder(topic,mapping,List.empty[String],List.empty[String])
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
          
      }
        
      // TODO - Profiling
      
      true
    
    } catch {
      case e:Exception => false
    }
    
  }
  
  def putSources(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = {
     
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
        writer.writeBulk(index, mapping, sources)      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }

}