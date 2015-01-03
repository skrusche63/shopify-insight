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

import org.elasticsearch.index.query._
import org.elasticsearch.search.{SearchHit,SearchHits}

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.JavaConversions._
/**
 * The ESQuestor holds a set of pre-defined Elasticsearch queries
 * and leverages these to retrieve data from predefined indexes
 */
object ESQuestor {

  /**
   * This is a convenience method to retrieve the aggregate for
   * a certain task identifier
   */
  def query_Aggregate(requestCtx:RequestContext,uid:String):InsightAggregate = {
    /*
     * Retrieve the aggregate record from the 'orders/aggregates' index, 
     * that matches the unique identifier
     */
    val fbuilder = FilterBuilders.termFilter(Names.UID_FIELD,uid)
    query_FilteredAggregate(requestCtx,fbuilder)
    
  }
  
  def query_FilteredAggregate(requestCtx:RequestContext,filterBuilder:FilterBuilder):InsightAggregate = {

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)
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
    requestCtx.JSON_MAPPER.readValue(hit, classOf[InsightAggregate])
    
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

    val result = hits.hits().map(x => requestCtx.JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightAggregate])).sortBy(x => x.timestamp)
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
    val fbuilder = FilterBuilders.termFilter(Names.UID_FIELD,uid)
    query_FilteredForecasts(requestCtx,fbuilder)
    
  }
  
  def query_FilteredForecasts(requestCtx:RequestContext,filterBuilder:FilterBuilder):List[InsightForecast] = {

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)

    val count = requestCtx.count("users","forecasts",qbuilder)    
    val response = requestCtx.find("users", "forecasts", qbuilder,count)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no forecast record for the respective identifier */
    if (total == 0) return List.empty[InsightForecast]
    
    val result = hits.hits().map(x => requestCtx.JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightForecast]))    
    result.toList
    
  }
  
  /**
   * This query retrieves all item records that refer to a certain preparation
   * task, specified by the respective unique identifier
   */
  def query_Items(requestCtx:RequestContext,uid:String):List[InsightItem] = {
    /*
     * Retrieve the item records from the 'users/items' index, 
     * that matches the unique identifier
     */
    val fbuilder = FilterBuilders.termFilter(Names.UID_FIELD,uid)
    query_FilteredItems(requestCtx,fbuilder)
    
  }
  
  def query_FilteredItems(requestCtx:RequestContext,filterBuilder:FilterBuilder):List[InsightItem] = {

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)

    val count = requestCtx.count("users","items",qbuilder)    
    val response = requestCtx.find("users", "items", qbuilder,count)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no item record for the respective identifier */
    if (total == 0) return List.empty[InsightItem]
    
    val result = hits.hits().map(x => requestCtx.JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightItem]))    
    result.toList
    
  }
  
  /**
   * This query retrieves all loyalty records that refer to a certain preparation
   * task, specified by the respective unique identifier
   */
  def query_Loyalties(requestCtx:RequestContext,uid:String):List[InsightLoyalty] = {
    /*
     * Retrieve the loyalty records from the 'users/loyalty' index, 
     * that matches the unique identifier
     */
    val fbuilder = FilterBuilders.termFilter(Names.UID_FIELD,uid)
    query_FilteredLoyalties(requestCtx,fbuilder)
    
  }
  
  def query_FilteredLoyalties(requestCtx:RequestContext,filterBuilder:FilterBuilder):List[InsightLoyalty] = {

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)

    val count = requestCtx.count("users","loyalties",qbuilder)    
    val response = requestCtx.find("users", "loyalties", qbuilder,count)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no loyalty record for the respective identifier */
    if (total == 0) return List.empty[InsightLoyalty]
    
    val result = hits.hits().map(x => requestCtx.JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightLoyalty]))    
    result.toList
    
  }
  
  def query_Recommendations(requestCtx:RequestContext,uid:String):List[InsightRecommendation] = {
    /*
     * Retrieve the recommendation records from the 'users/recommendations' index, 
     * that matches the unique identifier
     */
    val fbuilder = FilterBuilders.termFilter(Names.UID_FIELD,uid)
    query_FilteredRecommendations(requestCtx,fbuilder)
    
  }
  
  def query_FilteredRecommendations(requestCtx:RequestContext,filterBuilder:FilterBuilder):List[InsightRecommendation] = {

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)

    val count = requestCtx.count("users","recommendations",qbuilder)    
    val response = requestCtx.find("users", "recommendations", qbuilder,count)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no recommendation record for the respective identifier */
    if (total == 0) return List.empty[InsightRecommendation]
    
    val result = hits.hits().map(x => requestCtx.JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightRecommendation]))    
    result.toList
    
  }
  
  def query_Rules(requestCtx:RequestContext,uid:String):List[InsightRule] = {
    /*
     * Retrieve the rule records from the 'products/rules' index, 
     * that matches the unique identifier
     */
    val fbuilder = FilterBuilders.termFilter(Names.UID_FIELD,uid)
    query_FilteredRules(requestCtx,fbuilder)
    
  }
  
  def query_FilteredRules(requestCtx:RequestContext,filterBuilder:FilterBuilder):List[InsightRule] = {

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)

    val count = requestCtx.count("products","rules",qbuilder)    
    val response = requestCtx.find("products", "rules", qbuilder,count)
    
    val hits = response.getHits()
    val total = hits.totalHits()
    
    /* There is no rule record for the respective identifier */
    if (total == 0) return List.empty[InsightRule]
    
    val result = hits.hits().map(x => requestCtx.JSON_MAPPER.readValue(x.getSourceAsString,classOf[InsightRule]))    
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