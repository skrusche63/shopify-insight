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

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.RequestContext

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.{SearchHit,SearchHits}

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.JavaConversions._
/**
 * The ESQuestor holds a set of pre-defined Elasticsearch queries
 * and leverages these to retrieve data from predefined indexes
 */
object ESQuestor {

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  def query_Aggregate(requestCtx:RequestContext,uid:String):InsightAggregate = {
    /*
     * Retrieve the aggregate record from the 'orders/aggregates' index, 
     * that matches the unique identifier
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD,uid)
    val response = requestCtx.find("orders", "aggregates", qbuilder)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no aggregate for the respective identifier */
    if (total == 0) return null
    
    /* 
     * We expect to have a single aggregate for a certain
     * unique task identifier
     */
    val hit = hits.hits()(0).getSourceAsString
    JSON_MAPPER.readValue(hit, classOf[InsightAggregate])
    
  }

  def query_AllAggregates(requestCtx:RequestContext):List[InsightAggregate] = {

    val qbuilder = QueryBuilders.matchAllQuery()
    /*
     * Retrieval of the tasks is a two phase process, where
     * first the total number of tasks is determined, and
     * then the tasks are retrieved
     */
    val count = requestCtx.count("orders","aggregates",qbuilder)    
    val response = requestCtx.find("orders","aggregates",qbuilder,count)

    val hits = response.getHits()
    val total = hits.totalHits()
 
    if (total == 0) return List.empty[InsightAggregate]

    val result = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightAggregate])).sortBy(x => x.timestamp)
    result.toList
    
  }
  
  /**
   * This query retrieves all forecast records that refer to a certain preparation
   * task, specified by the respective unique identifier
   */
  def query_Forecasts(requestCtx:RequestContext,uid:String):List[InsightForecast] = {
    /*
     * Retrieve the forecast records from the 'users/forecasts' index, 
     * that matches the unique identifier
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD,uid)
    val response = requestCtx.find("users", "forecasts", qbuilder)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no forecast record for the respective identifier */
    if (total == 0) return List.empty[InsightForecast]
    
    val result = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightForecast]))    
    result.toList
    
  }
  
  /**
   * This query retrieves all loyalty records that refer to a certain preparation
   * task, specified by the respective unique identifier
   */
  def query_Loyalty(requestCtx:RequestContext,uid:String):List[InsightLoyalty] = {
    /*
     * Retrieve the loyalty records from the 'users/loyalty' index, 
     * that matches the unique identifier
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD,uid)
    val response = requestCtx.find("users", "loyalty", qbuilder)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no loyalty record for the respective identifier */
    if (total == 0) return List.empty[InsightLoyalty]
    
    val result = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightLoyalty]))    
    result.toList
    
  }
  
  /**
   * This query determines all entries from the database/tasks index
   * and can be used to determine a sorted list of time spans, where
   * each item describes a temporal snapshot the purchase history or
   * database synchronization.
   * 
   * Actualy 'prepare' & 'synchronize' tasks are supported
   */
  def query_AllTasks(requestCtx:RequestContext,filter:String):List[InsightTask] = {

    val qbuilder = QueryBuilders.matchAllQuery()
    /*
     * Retrieval of the tasks is a two phase process, where
     * first the total number of tasks is determined, and
     * then the tasks are retrieved
     */
    val count = requestCtx.count("database","tasks",qbuilder)    
    val response = requestCtx.find("database","tasks",qbuilder,count)
 
    val rawset = response.getHits().hits().map(task(_)).sortBy(_._3)
 
    val result = if (filter == "*") {
      rawset.map(x => InsightTask(uid(x._1),x._2,x._3,x._4,x._5))
    
    } else {      
      rawset.filter(x => x._1.split(":")(0) == filter).map(x => InsightTask(uid(x._1),x._2,x._3,x._4,x._5))
    
    }

    result.toList
    
  }

  private def uid(key:String) = key.split(":")(1)
  
  private def task(hit:SearchHit):(String,String,Long,String,String) = {
        
    val data = hit.getSource()
        
    val key = data("key").asInstanceOf[String]
    
    val task = data("task").asInstanceOf[String]
    val timestamp = data("timestamp").asInstanceOf[Long]
    
    /*
     * Retrieve description of the tasks' time span
     */
    val created_at_min = data("created_at_min").asInstanceOf[String]
    val created_at_max = data("created_at_max").asInstanceOf[String]

    (key,task,timestamp,created_at_min,created_at_max)
    
  }

}