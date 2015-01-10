package de.kp.shopify.insight.actor.collect
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

class CustomerSync(requestCtx:RequestContext) extends BaseActor(requestCtx) {

  override def receive = {

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
        val sources = customers.map(x=> toSource(req_params,x))
        
        writer.writeBulkJSON("database", "customers", ids, sources)
        writer.close()
        
        val end = new java.util.Date().getTime
        requestCtx.listener ! String.format("""[INFO][UID: %s] Customer base synchronization finished in %s ms.""",uid,(end-start).toString)
         
        val params = Map(Names.REQ_MODEL -> "CUSTOMER") ++ req_params

        context.parent ! SynchronizeFinished(params)
        context.stop(self)
        
      } catch {
        case e:Exception => {

          requestCtx.listener ! String.format("""[ERROR][UID: %s] Customer base synchronization failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params

          context.parent ! SynchronizeFailed(params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }

  private def toSource(params:Map[String,String],customer:Customer):XContentBuilder = {
    
    val timestamp = params("timestamp").toLong
    
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* site */
	builder.field("site",customer.site)
	
	/* id */
	builder.field("id",customer.id)
	
	/* first_name */
	builder.field("first_name",customer.firstName)
	
	/* last_name */
	builder.field("last_name",customer.lastName)
	
	/* signup_date */
	builder.field("signup_date",customer.created_at)
	
	/* last_sync */
	builder.field("last_sync",timestamp)

    builder.field("email",customer.emailAddress)
    builder.field("email_verified",customer.emailVerified)

    builder.field("accepts_marketing",customer.marketing)

    builder.field("operational_state",customer.state)

	builder.endObject()
    builder
    
  }
 
}