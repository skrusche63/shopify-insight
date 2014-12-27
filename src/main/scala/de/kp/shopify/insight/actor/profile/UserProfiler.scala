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
import org.elasticsearch.search.SearchHits

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.{FindContext,PrepareContext}

import de.kp.shopify.insight.actor._

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.source._

class UserProfiler(prepareContext:PrepareContext,findContext:FindContext) extends BaseActor {

  override def receive = {
   
    case message:StartProfile => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        prepareContext.listener ! String.format("""[INFO][UID: %s] User profile building started.""",uid)
        
      } catch {
        case e:Exception => {

          prepareContext.listener ! String.format("""[ERROR][UID: %s] User profile building failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! ProfileFailed(params)            
          context.stop(self)
          
        }
      
      }
    
    }
    
  }

  private def getForecasts(params:Map[String,String]):SearchHits = {
    /*
     * Retrieve the forecasts built before from the Elasticsearch 'orders/forecasts' index;
     * the only parameter that is required to retrieve the rules is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    val response = findContext.find("orders", "forecasts", qbuilder)

    response.getHits()
    
  }
  
  private def getLoyalty(params:Map[String,String]):SearchHits = {
    /*
     * Retrieve the loyalty built before from the Elasticsearch 'orders/loyalty' index;
     * the only parameter that is required to retrieve the rules is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    val response = findContext.find("orders", "loyalty", qbuilder)

    response.getHits()
    
  }
  
}