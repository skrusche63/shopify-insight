package de.kp.shopify.insight.collect
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

class PRDCollector(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {

  override def receive = {
    /*
     * Retrieve all Shopify products of a certain store via the 
     * REST interface and register them in an Elasticsearch index
     */
    case message:StartCollect => {
      
      val uid = params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      ctx.listener ! String.format("""[INFO][UID: %s] PRD collection request received at %s.""",uid,start)
      
      try {

        val writer = new ElasticWriter()

        if (writer.open("database","products") == false)
          throw new Exception("Product database cannot be opened.")
      
        ctx.listener ! String.format("""[INFO][UID: %s] PRD collection started.""",uid)
            
        val start = new java.util.Date().getTime            
        val products = ctx.getProducts(params)
       
        ctx.listener ! String.format("""[INFO][UID: %s] Product base loaded from store.""",uid)

        val ids = products.map(_.id)
        val sources = products.map(toSource(_))
        
        writer.writeBulkJSON("database", "products", ids, sources)
        writer.close()
        
        val end = new java.util.Date().getTime
        ctx.listener ! String.format("""[INFO][UID: %s] PRD collection finished at %s.""",uid,end.toString)
        
        val new_params = Map(Names.REQ_MODEL -> "PRD") ++ params

        context.parent ! CollectFinished(new_params)
        context.stop(self)
        
      } catch {
        case e:Exception => {

          ctx.listener ! String.format("""[ERROR][UID: %s] PRD collection failed due to an internal error.""",uid)
          
          val new_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ params

          context.parent ! CollectFailed(new_params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }

  private def toSource(product:Product):XContentBuilder = {
           
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* site */
	builder.field(Names.SITE_FIELD,product.site)
	
	/* id */
	builder.field("id",product.id)
	
	/* name */
	builder.field("name",product.name)
	
	/* category */
	builder.field("category",product.category)
	
	/* tags */
	builder.field("tags",product.tags)
	
	/* images */
	builder.startArray("images")
	
	for (image <- product.images) {
	  
	  builder.startObject()
	  
	  /* id */
	  builder.field("id",image.id)
	  
	  /* position */
	  builder.field("position",image.position)
	  
	  /* source */
	  builder.field("source",image.src)

	  builder.endObject()
	
	}
	
    builder.endArray()
	
	/* vendor */
	builder.field("vendor",product.vendor)
	
	builder.endObject()
   
    builder
    
  }

}