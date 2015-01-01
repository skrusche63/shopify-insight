package de.kp.shopify.insight.actor.profile
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

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.{SearchHit,SearchHits}

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight._

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.preference._

import scala.collection.JavaConversions._

class UserProfiler(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartProfile => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] User profile building started.""",uid)
       
        /*
         * Retrieve order items from the 'orders/items' index and determine 
         * most probable purchase day of week and time period of the day
         */
        val item_hits = items(req_params).hits().map(item(_))
        
        val daytime_hits = new TimePreference().build(item_hits).groupBy(x => (x._1,x._2))
        
        /*
         * Retrieve sales forecasts from the 'orders/forecasts' index
         * and transform into tuple representation; a certain 'user'
         * has multiple forecast steps assigned
         */
        val forecast_hits = forecasts(req_params).hits().map(forecast(_)).groupBy(x => (x._1,x._2))
        
        /*
         * Retrieve loyalty trajectories from the 'orders/loyalty' index
         * and transform into tuple representation; a certain user has a
         * single loyalty specification assigned
         */
        val loyalty_hits = loyalty(req_params).hits().map(trajectory(_)).groupBy(x => (x._1,x._2))
        /*
         * Retrieve recommendations from the 'orders/recommendations' index
         * and transform into tuple representation; a certain user has a single
         * product recommendation assigned
         */
        val recommendation_hits = recommendations(req_params).hits().map(recommendation(_)).groupBy(x => (x._1,x._2))
        
        requestCtx.listener ! String.format("""[INFO][UID: %s] Raw data for user profile building retrieved.""",uid)

        /*
         * Join the datasets into a single profile; to this end, we start
         * with the forecast data and append loyalty and recommendation part
         */	    
        val profiles = forecast_hits.map(kv => {

          val builder = XContentFactory.jsonBuilder()
	      builder.startObject()

          val k = kv._1
          val v = kv._2

          /* 
           * The 'forecast' description is directly extracted from
           * the values (v) provided by this dataset; all other parts
           * have to be looked up 
           */
          builder.startArray("forecast")
          for (record <- v) {
        
            builder.startObject()

            builder.field("step",record._3)
        
            builder.field("state",record._4)
            builder.field("amount",record._5)

            builder.field("days",record._6)
            builder.field("score",record._7)
        
            builder.endObject()
          
          }
      
          builder.endArray()

          /* loyalty */
          builder.startObject("loyalty")
          if (loyalty_hits.contains(k)) {
            
            val loyalty = loyalty_hits(k)(0)
            
            builder.field("low",loyalty._3)
            builder.field("norm",loyalty._4)

            builder.field("high",loyalty._5)
            
            builder.startArray("trajectory")
            loyalty._6.foreach(v => builder.value(v))
            builder.endArray
            
          }
          builder.endObject()
          
          /* preferences */
          builder.startObject("preference")
          
          /*
           * The 'day' preference combines the day of the 
           * week with a preference for a certain day to
           * purchase products
           */
          builder.startArray("day")
          if (daytime_hits.contains(k)) {
            
            val days = daytime_hits(k)(0)._3
            for (day <- days) {
              builder.startObject()
              
              builder.field("day",day._1)
              builder.field("score",day._2)
              
              builder.endObject()
            }
            
            
          }
          builder.endArray()

          builder.startArray("time")
          if (daytime_hits.contains(k)) {
            
            val times = daytime_hits(k)(0)._4
            for (time <- times) {
              builder.startObject()
              
              builder.field("time",time._1)
              builder.field("score",time._2)
              
              builder.endObject()
            }
            
            
          }
          builder.endArray()
          
          builder.endObject()
          
          /* recommendation */
          builder.startObject("recommendation")
          if (recommendation_hits.contains(k)) {
            
            val recommendation = recommendation_hits(k)(0)
            
            builder.field("support", recommendation._4)
            builder.field("total", recommendation._5)
            
            builder.field("confidence", recommendation._6)
            builder.field("weight", recommendation._7)
            
            builder.startArray("products")
            recommendation._3.foreach(v => builder.value(v))
            builder.endArray()
            
          }
          builder.endObject()
          
          builder.endObject()          
          builder
          
        }).toList
        
      } catch {
        case e:Exception => {

          requestCtx.listener ! String.format("""[ERROR][UID: %s] User profile building failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! ProfileFailed(params)            
          context.stop(self)
          
        }
      
      }
    
    }
    
  }

  private def forecasts(params:Map[String,String]):SearchHits = {
    /*
     * Retrieve all forecast records from the 'users/forecasts' index;
     * the only parameter that is required to retrieve the data is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    val response = requestCtx.find("users", "forecasts", qbuilder)

    response.getHits()
    
  }
  
  private def forecast(hit:SearchHit):(String,String,Int,String,Float,Int,Double) = {
        
    val data = hit.getSource()
        
    val site = data(Names.SITE_FIELD).asInstanceOf[String]
    val user = data(Names.USER_FIELD).asInstanceOf[String]
        
    val step = data(Names.STEP_FIELD).asInstanceOf[Int]
    val state = data(Names.STATE_FIELD).asInstanceOf[String]

    val amount = data(Names.AMOUNT_FIELD).asInstanceOf[Float]
    val days = data(Names.DAYS_FIELD).asInstanceOf[Int]

    val score = data(Names.SCORE_FIELD).asInstanceOf[Double]      

    (site,user,step,state,amount,days,score)
  
  }
  
  private def items(params:Map[String,String]):SearchHits = {
    /*
     * Retrieve all item records from the 'users/items' index;
     * the only parameter that is required to retrieve the data is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    val response = requestCtx.find("users", "items", qbuilder)

    response.getHits()
    
  }
  
  private def item(hit:SearchHit):(String,String,String,Long,Int) = {
        
    val data = hit.getSource()
        
    val site = data(Names.SITE_FIELD).asInstanceOf[String]
    val user = data(Names.USER_FIELD).asInstanceOf[String]
        
    val group = data(Names.GROUP_FIELD).asInstanceOf[String]
    val timestamp = data(Names.TIMESTAMP_FIELD).asInstanceOf[Long]
        
    val item = data(Names.ITEM_FIELD).asInstanceOf[Int]

    (site,user,group,timestamp,item)
  
  }
  
  private def loyalty(params:Map[String,String]):SearchHits = {
    /*
     * Retrieve all loyalty records from the 'users/loyalty' index;
     * the only parameter that is required to retrieve the data is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    val response = requestCtx.find("users", "loyalty", qbuilder)

    response.getHits()
    
  }
  
  private def trajectory(hit:SearchHit):(String,String,Double,Double,Double,List[String]) = {
        
    val data = hit.getSource()
        
    val site = data(Names.SITE_FIELD).asInstanceOf[String]
    val user = data(Names.USER_FIELD).asInstanceOf[String]
        
    val trajectory = data(Names.TRAJECTORY_FIELD).asInstanceOf[List[String]]
    
    /*
     * From the trajectory, we also derive the percentage of the different 
     * loyalty states. Note, that these loyalty rates are the counterpart to 
     * Sentiment Analysis, when content is considered
     */
    val rates = trajectory.groupBy(x => x).map(x => (x._1, x._2.length.toDouble / trajectory.length))    
    
    val low  = if (rates.contains("L")) rates("L") else 0.0 
    val norm = if (rates.contains("N")) rates("N") else 0.0 

    val high = if (rates.contains("H")) rates("H") else 0.0 
    
    (site,user,low,norm,high,trajectory)
  
  }
  
  private def recommendations(params:Map[String,String]):SearchHits = {
    /*
     * Retrieve all recommendation records from the 'users/recommendations' index;
     * the only parameter that is required to retrieve the data is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    val response = requestCtx.find("users", "recommendations", qbuilder)

    response.getHits()
    
  }
  
  private def recommendation(hit:SearchHit):(String,String,List[Int],Int,Long,Double,Double) = {
        
    val data = hit.getSource()
        
    val site = data(Names.SITE_FIELD).asInstanceOf[String]
    val user = data(Names.USER_FIELD).asInstanceOf[String]
        
    val consequent = data(Names.CONSEQUENT_FIELD).asInstanceOf[List[Int]]

    val support = data(Names.SUPPORT_FIELD).asInstanceOf[Int]
    val total   = data(Names.TOTAL_FIELD).asInstanceOf[Long]

    val confidence = data(Names.CONFIDENCE_FIELD).asInstanceOf[Double]      
    val weight = data(Names.WEIGHT_FIELD).asInstanceOf[Double]      

    (site,user,consequent,support,total,confidence,weight)

  }

}