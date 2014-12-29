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

import de.kp.shopify.insight.PrepareContext

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

class CustomerSync(prepareContext:PrepareContext) extends BaseActor {

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
      
        prepareContext.listener ! String.format("""[INFO][UID: %s] Customer base synchronization started.""",uid)
            
        val start = new java.util.Date().getTime            
        val customers = prepareContext.getCustomers(req_params)
       
        prepareContext.listener ! String.format("""[INFO][UID: %s] Customer base loaded.""",uid)

        val ids = customers.map(_.id)
        val sources = customers.map(toSource(_))
        
        writer.writeBulkJSON("database", "customers", ids, sources)
        writer.close()
        
        val end = new java.util.Date().getTime
        prepareContext.listener ! String.format("""[INFO][UID: %s] Customer base synchronization finished in %s ms.""",uid,(end-start).toString)
         
        context.parent ! SynchronizeFinished(req_params)
        
      } catch {
        case e:Exception => {

          prepareContext.listener ! String.format("""[ERROR][UID: %s] Customer base synchronization failed due to an internal error.""",uid)
          
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
	
	// TODO
	builder.endObject()
   
    builder
    
  }
}