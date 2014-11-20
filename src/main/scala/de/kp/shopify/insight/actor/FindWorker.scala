package de.kp.shopify.insight.actor
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

import de.kp.shopify.insight.{RemoteContext,ShopifyContext}
import de.kp.shopify.insight.io.RequestBuilder

import de.kp.shopify.insight.model._

import scala.util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer,HashMap}

class FindWorker(ctx:RemoteContext) extends WorkerActor(ctx) {

  private val stx = new ShopifyContext()
  
  override def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      
      /*
       * STEP #1: Retrieve data from remote predictive engine
       */
      val service = req.service
      val message = Serializer.serializeRequest(req)
      
      try {
        
        val response = getResponse(service,message)     
        response.onSuccess {
        
          case result => {
            /*
             * STEP #2: Aggregate data from predictive engine with
             * respective shopify data
             */
            val intermediate = Serializer.deserializeResponse(result)
            origin ! buildResponse(req,intermediate)
        
          }

        }
        response.onFailure {
          case throwable => origin ! failure(req,throwable.getMessage)	 	      
	    }
        
      } catch {
        case e:Exception => origin ! failure(req,e.getMessage)
      }
    }
    
  }

  private def buildResponse(request:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    request.service match {
      
      case Services.ASSOCIATION => {
        
        request.task.split(":")(1) match {
          
          case Tasks.RECOMMENDATION => {
            /* 
             * The total number of products returned as recommendations 
             * for each user
             */
            val total = request.data("total").toInt
            /*
             * A recommendation request is dedicated to a certain 'site'
             * and a list of users, and the result is a list of rules
             * assigned to this input
             */
            val rules = Serializer.deserializeMultiUserRules(request.data(Tasks.RECOMMENDATION)).items
            val recomms = rules.map(entry => {
              
              val (site,user) = (entry.site,entry.user)
              /*
               * Note, that weighted rules are determined by providing a certain threshold;
               * to determine the respective items, we first take those items with the heighest 
               * weight, highest confidence and finally highest support
               */
              val items = getItems(entry.items,total)
              new Recommendation(site,user,getProducts(items))
              
            })
            
            new Recommendations(request.data("uid"),recomms)
          
          }
          /*
           * In case of all other tasks, response is directly sent 
           * to the requestor without any further aggregation
           */          
          case _ => intermediate
        }
        
      }
      /*
       * In case of all other services or engines invoked, response is
       * directly sent to the requestor without any further aggregation
       */
      case _ => intermediate

    }
    
  }

  private def getItems(rules:List[WeightedRule],total:Int):List[Int] = {
    
    val dataset = rules.map(rule => {
      (rule.weight,rule.confidence,rule.support,rule.consequent)
    })
    
    val sorted = dataset.sortBy(x => (-x._1, -x._2, -x._3))
    val len = sorted.length
    
    if (len == 0) return List.empty[Int]
    
    var items = List.empty[Int]
    breakable {
      
      (0 until len).foreach( i => {
        
        items = items ++ sorted(i)._4
        if (items.length >= total) break
      
      })
      
    }

    items

  }
  
  private def getProducts(items:List[Int]):List[ShopifyProduct] = {
    
    val products = ArrayBuffer.empty[ShopifyProduct]
    try {
      
      for (item <- items) {
        products += stx.getProduct(item.toLong)
      }
      
    } catch {
      case e:Exception => {/* do nothing */}
    }
    
    products.toList
    
  }
}