package de.kp.shopify.insight.actor.synchronize
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import de.kp.spark.core.Names
import de.kp.spark.core.io._

import de.kp.shopify.insight._

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

class OrderSync(requestCtx:RequestContext) extends BaseActor {

  override def receive = {

    case message:StartSynchronize => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {

        val writer = new ElasticWriter()

        if (writer.open("database","orders") == false)
          throw new Exception("Order database cannot be opened.")
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Order base synchronization started.""",uid)
            
        val start = new java.util.Date().getTime            
        val orders = requestCtx.getOrders(req_params)
       
        requestCtx.listener ! String.format("""[INFO][UID: %s] Order base loaded.""",uid)

        val sources = orders.map(x=> toSource(req_params,x))
        
        writer.writeBulkJSON("database", "customers", sources)
        writer.close()
        
        val end = new java.util.Date().getTime
        requestCtx.listener ! String.format("""[INFO][UID: %s] Order base synchronization finished in %s ms.""",uid,(end-start).toString)
        
        val params = Map(Names.REQ_MODEL -> "ORDER") ++ req_params

        context.parent ! SynchronizeFinished(params)
        context.stop(self)
        
      } catch {
        case e:Exception => {

          requestCtx.listener ! String.format("""[ERROR][UID: %s] Order base synchronization failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params

          context.parent ! SynchronizeFailed(params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }

  private def toSource(params:Map[String,String],order:Order):XContentBuilder = {
    
    val uid = params("uid")
    val timestamp = params("timestamp").toLong
           
    val created_at_min = params("created_at_min")
    val created_at_max = params("created_at_max")
    
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
               
   /********** METADATA **********/
    
	/* uid */
	builder.field("uid",uid)
	
	/* last_sync */
	builder.field("last_sync",timestamp)
	
	/* created_at_min */
	builder.field("created_at_min",created_at_min)
	
	/* created_at_max */
	builder.field("created_at_max",created_at_max)
	
	/* site */
	builder.field("site",order.site)
             
    /********** ORDER DATA **********/
	
	/* user */
	builder.field("user",order.user)
	
	/* amount */
	builder.field("amount",order.amount)
	
	/* timestamp */
	builder.field("timestamp",order.timestamp)
	
	/* group */
	builder.field("group",order.group)
	
	/* ip_address */
	builder.field("ip_address",order.ip_address)
	
	/* user_agent */
	builder.field("user_agent",order.user_agent)
 	
	/* items */
	builder.startArray("items")
	
	for (item <- order.items) {
	  
	  builder.startObject()
	  
	  /* item */
	  builder.field("item",item.item)
	  
	  /* quantity */
	  builder.field("quantity",item.quantity)
	  
	  builder.endObject()
	  
	}
	
    builder.endArray()
	
	builder.endObject()	
	builder

  }

}