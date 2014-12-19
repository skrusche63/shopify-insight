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

import akka.actor.ActorRef

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.{RemoteContext,ShopifyContext}

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

import de.kp.shopify.insight.source._

import scala.util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer,HashMap}

class RemoteQuestor(ctx:RemoteContext,listener:ActorRef) extends BaseActor {

  private val stx = new ShopifyContext()
  
  override def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      
      val uid = req.data(Names.REQ_UID)
      val Array(task,topic) = req.task.split(":")
      
      try {
      
        val (service,message) = topic match {
        
          case "cross-sell" => {
            /* 
             * Build service request message to invoke remote Association Analysis engine; 
             * the 'cross-sell' request is mapped onto an 'antecendent' task
             */
            val service = "association"
            val task = "get:antecedent"

            val data = new CrossSellBuilder().get(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case "loyalty" => {
            /*
             * Build service request message to invoke remote Intent Recognition engine;
             * the 'loyalty' request is mapped onto an 'observation' task
             */
            val service = "intent"
            val task = "get:observation"

            val data = new LoyaltyBuilder().get(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case "placement" => {
             /* 
             * Build service request message to invoke remote Association Analysis engine; 
             * the 'placement' request is mapped onto an 'antecendent' task
             */
            val service = "association"
            val task = "get:antecedent"

            val data = new PlacementBuilder().get(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
         }
        
          case "purchase" => {
            /*
             * Build service request message to invoke remote Intent Recognition engine;
             * the 'purchase' request is mapped onto a 'state' task
             */
            val service = "intent"
            val task = "get:state"

            val data = new PurchaseBuilder().get(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case "recommendation" => {
             /* 
             * Build service request message to invoke remote Association Analysis engine; 
             * the 'recommendation' request is mapped onto a 'transaction' task
             */
            val service = "association"
            val task = "get:transaction"

            val data = new RecommendationBuilder().get(req.data)
            val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
            (service,message)
            
          }
        
          case _ => throw new Exception(String.format("""[UID: %s] Unknown topic received.""",uid))
        
        }
        
        val response = getResponse(service,message)     
        response.onSuccess {
        
          case result => {
 
            val intermediate = Serializer.deserializeResponse(result)
            
            origin ! buildResponse(req,intermediate)
            context.stop(self)
        
          }

        }
        response.onFailure {
          case throwable => {
            origin ! failure(req,throwable.getMessage)	 
            context.stop(self)
          }
	    }
      
      } catch {
        case e:Exception => {
          origin ! failure(req,e.getMessage)
          context.stop(self)
        }
        
      }
      
    }
    
  }

  private def getResponse(service:String,message:String) = ctx.send(service,message).mapTo[String] 

  private def buildResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    val Array(task,topic) = req.task.split(":")
    topic match {
 
      case "cross-sell" => {

        val rules = Serializer.deserializeRules(intermediate.data(Names.REQ_RESPONSE))
        /*
         * Turn retrieved association rules into a list of Shopify products, which
         * then are returned to requestor as a result of this request.
         * 
         * 'total' is an external request parameter and specifies how many products
         * have to be taken into account to build the product list
         */        
        val total = req.data(Names.REQ_TOTAL).toInt
        val items = getItems(rules.items,total)
          
        new CrossSell(getProducts(items))
       
      }
      
      case "loyalty" => {
        // TODO
      }
      
      case "placement" => {

        val rules = Serializer.deserializeRules(intermediate.data(Names.REQ_RESPONSE))
        /*
         * Turn retrieved association rules into a list of Shopify products, which
         * then are returned to requestor as a result of this request.
         * 
         * 'total' is an external request parameter and specifies how many products
         * have to be taken into account to build the product list
         */        
        val total = req.data(Names.REQ_TOTAL).toInt
        val items = getItems(rules.items,total)
        
        new Placement(getProducts(items))        
      
      }
      
      case "purchase" => {
        /*
         * A set of Markovian rules (i.e. a relation between a certain state and a sequence
         * of most probable subsequent states) is transformed into a list of user specific
         * purchase forecasts
         */
        val rules = Serializer.deserializeMarkovRules(intermediate.data(Names.REQ_RESPONSE))
        /*
         * Transform the rules in an appropriate lookup format as the states sent
         * to the Intent Recognition engine are distinct
         */
        val lookup = rules.items.map(rule => (rule.antecedent,rule.consequent)).toMap
        /*
         * The questor provides a list of users and assigned lists of last purchase
         * time, amount and states
         */
        val users = req.data(Names.REQ_USERS).split(",").toList
        
        val amounts = req.data(Names.REQ_AMOUNTS).split(",").toList
        val times = req.data(Names.REQ_TIMES).split(",").toList
        
        val states = req.data(Names.REQ_STATES).split(",").toList     

        val dataset = users.zip(amounts).zip(times).zip(states).map{case (((a,b),c),d) => (a,b.toFloat,c.toLong,d)}
        val forecasts = dataset.map(entry => {
          
          val (user,amount,time,state) = entry
          UserForecast(user,forecast(amount,time,lookup(state)))
          
        })

        UserForecasts(forecasts)
        
      }
      
      case "recommendation" => {
        /*
         * A recommendation request is dedicated to a certain 'site' and a list of users, 
         * and the result is a list of rules assigned to this input
         */
        val rules = Serializer.deserializeMultiUserRules(intermediate.data(Names.REQ_RESPONSE))
        /*
         * Turn retrieved user rules rules into a list of Shopify products, which
         * then are returned to requestor as a result of this request.
         * 
         * 'total' is an external request parameter and specifies how many products
         * have to be taken into account to build the product list
         */        
        val total = req.data(Names.REQ_TOTAL).toInt
        val recommendations = rules.items.map(entry => {
              
          val (site,user) = (entry.site,entry.user)
          /*
           * Note, that weighted rules are determined by providing a certain threshold;
           * to determine the respective items, we first take those items with the heighest 
           * weight, highest confidence and finally highest support
           */
          val items = getWeightedItems(entry.items,total)
          new Recommendation(site,user,getProducts(items))
          
        })
           
        new Recommendations(recommendations)

      }
      
      /*
       * In case of all other tasks, response is directly sent 
       * to the requestor without any further aggregation
      */          
      case _ => intermediate
       
    }

  }
  /*
   * The Intent Recognition engine returns a list of Markovian states; the ordering
   * of these states reflects the number of steps looked ahead
   */
  private def forecast(amount:Float,time:Long,states:List[MarkovState]):List[Forecast] = {
    
    val result = ArrayBuffer.empty[Forecast]
    val steps = states.size
    
    if (steps == 0) return result.toList
    
    val record = states.head
    
    val next_time = AmountHandler.nextDate(record.name, time)
    val next_amount = AmountHandler.nextAmount(record.name, amount)
    
    result += Forecast(next_amount,next_time,record.probability)

    var pre_time = next_time
    var pre_amount = next_amount
    
    for (record <- states.tail) {

      val next_time = AmountHandler.nextDate(record.name, pre_time)
      val next_amount = AmountHandler.nextAmount(record.name, pre_amount)
   
      result += Forecast(next_amount,next_time,record.probability)

      pre_time = next_time
      pre_amount = next_amount
 
    }
    
    result.toList
    
  }
  
  /**
   * This private method returns items from a list of association rules
   */
  private def getItems(rules:List[Rule],total:Int):List[Int] = {
    
    val dataset = rules.map(rule => {
      (rule.confidence,rule.support,rule.consequent)
    })
    
    val sorted = dataset.sortBy(x => (-x._1, -x._2))
    val len = sorted.length
    
    if (len == 0) return List.empty[Int]
    
    var items = List.empty[Int]
    breakable {
      
      (0 until len).foreach( i => {
        
        items = items ++ sorted(i)._3
        if (items.length >= total) break
      
      })
      
    }

    items
    
  }
  
  /**
   * This private method returns items from a list of weighted association 
   * rules; the weight is used to specify the intersection of rule based
   * antecedent and last customer transaction items
   */
  private def getWeightedItems(rules:List[WeightedRule],total:Int):List[Int] = {
    
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