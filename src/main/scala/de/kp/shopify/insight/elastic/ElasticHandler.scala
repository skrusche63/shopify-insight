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
      
      /*
       * This method is used to create a specific Elasticsearch index; to this end, we
       * have to distinguish between core topics, i.e. information objects that are
       * supported by the core projects, and those that depend on specific application
       * logic
       */
      val core_topics = List("amount","event","item","feature","product","rule","sequence","state")
      if (core_topics.contains(topic)) {
        /*
         * Core information elements get indexed during the collection sub process,
         * and have to match with one of the topics specified above, as no other
         * topics are supported by the Predictiveworsk engines
         */
        val builder = ElasticBuilderFactory.getBuilder(topic,mapping,List.empty[String],List.empty[String])
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
      
        /*
         * Raw data that are ingested by the tracking functionality do not have
         * to be specified by a field or metadata specification; we therefore
         * and the field specification here as an internal feature
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
     
      } else {
        
        if (topic == "forecast") {
          /*
           * This Elasticsearch index registers purchase or sale forecasts;
           * in this case no additional fields have to be created as no
           * Predictiveworks engine will ever request these data
           */
          val builder = new ElasticForecastBuilder().createBuilder(mapping)
          val indexer = new ElasticIndexer()
    
          indexer.create(index,mapping,builder)
          indexer.close()
        
        } else if (topic == "loyalty") {
          /*
           * This Elasticsearch index registers loyalty trajectories;
           * in this case no additional fields have to be created as no
           * Predictiveworks engine will ever request these data
           */
          val builder = new ElasticLoyaltyBuilder().createBuilder(mapping)
          val indexer = new ElasticIndexer()
    
          indexer.create(index,mapping,builder)
          indexer.close()
          
        } else if (topic == "recommendation") {
          /*
           * This Elasticsearch index registers product recommendations;
           * in this case no additional fields have to be created as no
           * Predictiveworks engine will ever request these data
           */
          val builder = new ElasticRecommendationBuilder().createBuilder(mapping)
          val indexer = new ElasticIndexer()
    
          indexer.create(index,mapping,builder)
          indexer.close()
          
        }
        
      }
      
      true
    
    } catch {
      case e:Exception => false
    }
    
  }
  /**
   * Put 'amounts' to the Elasticsearch 'amount' index; this method is called during
   * the 'collect' sub process
   */  
  def putAmount(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = putSources(index,mapping,sources)
  /**
   * Put 'forecasts' to the Elasticsearch 'forecasts' index; this method is called during
   * the 'enrich' sub process
   */    
  def putForecasts(index:String,mapping:String,dataset:List[Map[String,String]]):Boolean = {
     
    try {
    
      val writer = new ElasticWriter()
        
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        val builder = new ElasticForecastBuilder()
        val sources = dataset.map(builder.createSource(_))
        /*
         * Writing this source to the respective index throws an
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
  /**
   * Put 'items' to the Elasticsearch 'items' index; this method is called during
   * the 'collect' sub process
   */
  def putItems(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = putSources(index,mapping,sources)
  /**
   * Put 'loyalty' to the Elasticsearch 'loyalty' index; this method is called during
   * the 'enrich' sub process
   */
  def putLoyalty(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = putSources(index,mapping,sources)
  /**
   * Put 'recommendations' to the Elasticsearch 'recommendations' index; this method is called during
   * the 'enrich' sub process
   */  
  def putRecommendations(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = putSources(index,mapping,sources)  
  /**
   * Put 'rules' to the Elasticsearch 'rules' index; this method is called during
   * the 'enrich' sub process
   */  
  def putRules(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = putSources(index,mapping,sources)  
  /**
   * Put 'states' to the Elasticsearch 'states' index; this method is called during
   * the 'collect' sub process
   */  
  def putStates(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = putSources(index,mapping,sources)

  private def putSources(index:String,mapping:String,sources:List[java.util.Map[String,Object]]):Boolean = {
     
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