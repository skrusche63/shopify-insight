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
        val sources = customers.map(x=> toSource(req_params,x))
        
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
  /**
   * This method merges customer details for existing customers
   * and replaces the existing record with the merged one; note,
   * that with this method, we even hold customers which have
   * already been deleted from the Shopify store.
   * 
   * The customer base controlled by the Elasticsearch index
   * database/customers is the starting point for any kind of
   * customer lifecycle evaluation 
   */
  private def toSource(params:Map[String,String],customer:Customer):XContentBuilder = {
    
    val timestamp = params("timestamp").toLong
    
    val created_at_min = params("created_at_min")
    val created_at_max = params("created_at_max")
  
    /*
     * Determine whether this customer is a new or an
     * already existing one
     */
    val customer_json = requestCtx.getAsString("database","customers",customer.id)  
    val customer_ds = if (customer_json == null) null else requestCtx.JSON_MAPPER.readValue(customer_json,classOf[InsightCustomer])
    
    /*
     * Compute timeseries from history and actual data record
     */
    val history_ds = if (customer_ds == null) List.empty[(Float,Long,Long)] else customer_ds.customer_data.map(x => (x.amount_spent,x.orders_count,x.timestamp))
    
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
	
	/* last_update */
	builder.field("last_update",timestamp)

    builder.field("email",customer.emailAddress)
    builder.field("email_verified",customer.emailVerified)

    builder.field("accepts_marketing",customer.marketing)
	
	/* customer_data */
	builder.startArray("customer_data")
	
	/*
	 * Add customer data history data
	 */
	if (customer_ds != null) {
	   
	  val customer_data = customer_ds.customer_data
	  for (customer_detail <- customer_data) {
	    
	    builder.startObject()

	    builder.field("timestamp",customer_detail.timestamp)
 
	    builder.field("created_at_min",customer_detail.created_at_min)
	    builder.field("created_at_max",customer_detail.created_at_max)

 	    builder.field("amount_spent",customer_detail.amount_spent)

	    builder.field("last_order",customer_detail.last_order)
 	    builder.field("orders_count",customer_detail.orders_count)

	    builder.field("operational_state",customer_detail.operational_state)
	    
	    builder.endObject()
	    
	  }
	}
    /*
     * Add new customer detail record
     */
	builder.startObject()

	builder.field("timestamp",timestamp)
 
	builder.field("created_at_min",created_at_min)
	builder.field("created_at_max",created_at_max)

 	builder.field("amount_spent",customer.totalSpent)

	builder.field("last_order",customer.lastOrder)
 	builder.field("orders_count",customer.ordersCount)

	builder.field("operational_state",customer.state)
	    
	builder.endObject()
	
	builder.endArray()

	builder.endObject()
    builder
    
  }

  private def timeseries(history_ds:List[(Float,Long,Long)],new_ds:(Float,Long,Long)) {
  
    val rawset = history_ds ++ List(new_ds).sortBy(_._3)
    val series = List(rawset.head) ++ (if (rawset.size > 1) {
      /* Determine total amount & orders count per timestamp */
      rawset.zip(rawset.tail).map(x => (x._2._1 - x._1._1,x._2._2 - x._1._2, x._2._3))
      
    } else List.empty[(Float,Long,Long)])
  
  }
  
}