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

import de.kp.shopify.insight.RequestContext

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.{SearchHit,SearchHits}

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.JavaConversions._
/**
 * The ESQuestor holds a set of pre-defined Elasticsearch queries
 * and leverages these to retrieve data from predefined indexes
 */
object ESQuestor {

  /**
   * This query determines all entries from the database/tasks index
   * and can be used to determine a sorted list of time spans, where
   * each item describes a temporal snapshot the purchase history or
   * database synchronization.
   * 
   * Actualy 'prepare' & 'synchronize' tasks are supported
   */
  def query_AllTasks(requestCtx:RequestContext,filter:String) {

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
      rawset.map(x => (uid(x._1),x._2,x._3,x._4,x._5))
    
    } else {      
      rawset.filter(x => x._1.split(":")(0) == filter).map(x => (uid(x._1),x._2,x._3,x._4,x._5))
    
    }

    // TODO
    
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