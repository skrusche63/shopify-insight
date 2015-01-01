package de.kp.shopify.insight.actor.enrich
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
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.elastic._

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * RelationModeler is an actor that uses an association rule model, transforms
 * the model into a product rule model (PRM) and and registers the result in an
 * ElasticSearch index.
 * 
 * This is part of the 'enrich' sub process that represents the third component of 
 * the data analytics pipeline.
 * 
 */
class RelationModeler(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
        
        requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model building started.""",uid)
        
        val (service,request) = buildRemoteRequest(req_params)

        val response = requestCtx.getRemoteContext.send(service,request).mapTo[String]            
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {

              val sources = toSources(req_params,res)

              if (requestCtx.putSources("products","rules",sources) == false)
                throw new Exception("Indexing processing has been stopped due to an internal error.")

              requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PRELAM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
            
            }
          
          }
          
        }
        /*
         * The Association Analysis engine returned an error message
         */
        response.onFailure {
          case throwable => {
                    
            requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
         
      } catch {
        case e:Exception => {
                    
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }

  private def buildRemoteRequest(params:Map[String,String]):(String,String) = {

    val service = "association"
    val task = "get:rule"

    val data = new ASRHandler().get(params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  
  private def toSources(params:Map[String,String],response:ServiceResponse):List[XContentBuilder] = {
            
    val uid = response.data(Names.REQ_UID)
    val rules = Serializer.deserializeRules(response.data(Names.REQ_RESPONSE))
    
    /*
     * Determine timestamp for the actual set of rules to be indexed
     */
    val now = new java.util.Date()
    val timestamp = now.getTime()
    
    rules.items.map(rule => {
      
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))

	  /* created_at_min */
	  builder.field("created_at_min",params("created_at_min"))

	  /* created_at_max */
	  builder.field("created_at_max",params("created_at_max"))
	  
	  /* antecedent */
	  builder.startArray("antecedent")
	  rule.antecedent.foreach(v => builder.value(v))
	  builder.endArray()
	  
	  /* consequent */
	  builder.startArray("consequent")
	  rule.antecedent.foreach(v => builder.value(v))
	  builder.endArray()

	  /* support */
	  builder.field("support",rule.support)
	  
	  /* total */
	  builder.field("total",rule.total)
	  
	  /* confidence */
	  builder.field("confidence",rule.confidence)
	  
	  builder.endObject()
      builder
      
    })
    
  }
  
}