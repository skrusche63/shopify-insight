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

class TaskQuestor(requestCtx:RequestContext) extends BaseActor(requestCtx) {

  def receive = {
    
    case query:TaskQuery => {

      val req_params = query.data
      val uid = req_params(Names.REQ_UID)
      
      val origin = sender
      try {
            
        requestCtx.listener ! String.format("""[INFO][UID: %s] All tasks request received.""",uid)
       
//        val tasks = ESQuestor.query_AllTasks(requestCtx, "*")
//        origin ! InsightTasks(tasks)
        
        context.stop(self)
      
      } catch {
        case e:Exception => {
            
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Task query failed: %s.""",uid,e.getMessage)

          val created_at_min = req_params("created_at_min")
          val created_at_max = req_params("created_at_max")

          origin ! SimpleResponse(uid,created_at_min,created_at_max,e.getMessage)
          
          context.stop(self)
          
        }
      
      }
      
    }

  }

}