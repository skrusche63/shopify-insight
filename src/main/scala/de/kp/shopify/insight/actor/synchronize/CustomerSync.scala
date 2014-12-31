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

class CustomerSync(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
    /*
     * Retrieve all Shopify customers of a certain store via the 
     * REST interface and register them in an Elasticsearch index
     */
    case message:StartSynchronize => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {

        val writer = new ElasticWriter()

        if (writer.open("database","customers") == false)
          throw new Exception("Customer database cannot be opened.")
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Customer base synchronization started.""",uid)
            
        val start = new java.util.Date().getTime            
        val customers = requestCtx.getCustomers(req_params)
       
        requestCtx.listener ! String.format("""[INFO][UID: %s] Customer base loaded.""",uid)

        val ids = customers.map(_.id)
        val sources = customers.map(toSource(_))
        
        writer.writeBulkJSON("database", "customers", ids, sources)
        writer.close()
        
        val end = new java.util.Date().getTime
        requestCtx.listener ! String.format("""[INFO][UID: %s] Customer base synchronization finished in %s ms.""",uid,(end-start).toString)
         
        context.parent ! SynchronizeFinished(req_params)
        
      } catch {
        case e:Exception => {

          requestCtx.listener ! String.format("""[ERROR][UID: %s] Customer base synchronization failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! SynchronizeFailed(params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }

  private def toSource(customer:Customer):XContentBuilder = {
           
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* site */
	builder.field(Names.SITE_FIELD,customer.site)
	
	/* id */
	builder.field("id",customer.id)
	
	/* first_name */
	builder.field("first_name",customer.firstName)
	
	/* last_name */
	builder.field("last_name",customer.lastName)
	
	/* email */
	builder.field("email",customer.emailAddress)
	
	/* email_verified */
	builder.field("email_verified",customer.emailVerified)
	
	/* accepts_marketing */
	builder.field("accepts_marketing",customer.marketing)
	
	/* operational_state */
	builder.field("operational_state",customer.state)
	
	/* last_order */
	builder.field("last_order",customer.lastOrder)
	
	/* orders_count */
	builder.field("orders_count",customer.ordersCount)
	
	/* amount_spent */
	builder.field("amount_spent",customer.totalSpent)
	
	builder.endObject()
   
    builder
    
  }

}