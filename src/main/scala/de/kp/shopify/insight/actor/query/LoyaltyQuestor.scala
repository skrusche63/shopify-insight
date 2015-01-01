package de.kp.shopify.insight.actor.query
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

import de.kp.shopify.insight.RequestContext
import de.kp.shopify.insight.actor.BaseActor

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.model._

import org.elasticsearch.index.query._
import scala.collection.mutable.Buffer

class LoyaltyQuestor(requestCtx:RequestContext) extends BaseActor {

  def receive = {
    
    case query:LoyaltyQuery => {

      val req_params = query.data
      val uid = req_params(Names.REQ_UID)
      
      val origin = sender
      try {
       
        val method = req_params(Names.REQ_METHOD)
        method match {
          
          case "user_loyalty" => {
            
            val filters = Buffer.empty[FilterBuilder]
            /*
             * This method retrieves the loyalty description of a certain user,
             * specified by 'site' and 'user' identifier 
             */
            val site = req_params(Names.REQ_SITE)
            filters += FilterBuilders.termFilter(Names.REQ_SITE, site)
            
            val user = req_params(Names.REQ_USER)
            filters += FilterBuilders.termFilter(Names.REQ_USER, user)
    
            if (req_params.contains(Names.REQ_UID))
              filters += FilterBuilders.termFilter(Names.REQ_UID, req_params(Names.REQ_UID))
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
            
            val loyalties = ESQuestor.query_FilteredLoyalties(requestCtx,fbuilder)
            origin ! InsightLoyalties(loyalties)
        
            context.stop(self)
            
          }
          
          case _ => throw new Exception("The request method '" + method + "' is not supported.")
          
        }
      
      } catch {
        case e:Exception => {
            
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Loyalty query failed: %s.""",uid,e.getMessage)
          origin ! SimpleResponse(uid,e.getMessage)
          
          context.stop(self)
          
        }
      
      }
      
    }

  }

}