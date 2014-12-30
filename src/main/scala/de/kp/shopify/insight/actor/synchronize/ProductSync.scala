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

class ProductSync(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
    /*
     * Retrieve all Shopify products of a certain store via the 
     * REST interface and register them in an Elasticsearch index
     */
    case message:StartSynchronize => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {

        val writer = new ElasticWriter()

        if (writer.open("database","products") == false)
          throw new Exception("Product database cannot be opened.")
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Product base synchronization started.""",uid)
            
        val start = new java.util.Date().getTime            
        val products = requestCtx.getProducts(req_params)
       
        requestCtx.listener ! String.format("""[INFO][UID: %s] Product base loaded.""",uid)

        val ids = products.map(_.id)
        val sources = products.map(toSource(_))
        
        writer.writeBulkJSON("database", "products", ids, sources)
        writer.close()
        
        val end = new java.util.Date().getTime
        requestCtx.listener ! String.format("""[INFO][UID: %s] Product base synchronization finished in %s ms.""",uid,(end-start).toString)
         
        context.parent ! SynchronizeFinished(req_params)
        
      } catch {
        case e:Exception => {

          requestCtx.listener ! String.format("""[ERROR][UID: %s] Product base synchronization failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! SynchronizeFailed(params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }

  private def toSource(product:Product):XContentBuilder = {
           
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	// TODO
	builder.endObject()
   
    builder
    
  }
}