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

import de.kp.shopify.insight.analytics._

import scala.collection.mutable.{ArrayBuffer,HashMap}
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * ForecastModeler is an actor that analyzes Shopify orders of a certain time interval 
 * (last 30 days from now on) and computes from the last two transactions on a per 
 * user basis a forecast of n steps with respect to next amount and datetime
 * 
 * This is part of the 'enrich' sub process that represents the third component of 
 * the data analytics pipeline.
 * 
 */
class ForecastModeler(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Purchase forecast model building started.""",uid)
        
        /*
         * STEP #1: Transform Shopify orders into purchases; these purchases are used 
         * to compute n-step ahead forecasts with respect to purchase amount and time
         */
        val purchases = transform(requestCtx.getOrders(req_params))
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Orders successfully transformed into purchases.""",uid)

        /*
         * STEP #2: Retrieve Markovian rules from Intent Recognition engine, combine
         * rules and purchases into a user specific set of purchase forecasts
         * 
         */
        val (service,req) = buildRemoteRequest(req_params,purchases.map(_._5))
        val response = requestCtx.getRemoteContext.send(service,req).mapTo[String]     
        
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {

              val sources = toSources(req_params,res,purchases)

              if (requestCtx.putSources("users","forecasts",sources) == false)
                throw new Exception("Indexing processing has been stopped due to an internal error.")

              requestCtx.listener ! String.format("""[INFO][UID: %s] Purchase forecast model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "UFORCM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
        
            }
            
          }

        }
        response.onFailure {
          case throwable => {

            requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an internal error.""",uid)
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
        
      } catch {
        case e:Exception => {

          requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }

  }  

  private def toSources(params:Map[String,String],response:ServiceResponse,purchases:List[(String,String,Float,Long,String)]):List[XContentBuilder] = {

    val uid = response.data(Names.REQ_UID)
    val timestamp = new java.util.Date().getTime
    
    /*
     * A set of Markovian rules (i.e. a relation between a certain state and a sequence
     * of most probable subsequent states) is transformed into a list of user specific
     * purchase forecasts
     */
    val rules = Serializer.deserializeMarkovRules(response.data(Names.REQ_RESPONSE))
    /*
     * Transform the rules in an appropriate lookup format as the states sent
     * to the Intent Recognition engine are distinct
     */
    val lookup = rules.items.map(rule => (rule.antecedent,rule.consequent)).toMap
    /*
     * Compute next probable purchase amount and time by combining the Markovian
     * rules and the purchases retrieved from the Shopify store
     */
    purchases.flatMap(p => {
      
      val (site,user,amount,time,state) = p
      
      val forecasts = buildForecasts(amount,time,lookup(state)).zipWithIndex
      forecasts.map(x => {
          
        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
       
        /* uid */
        builder.field(Names.UID_FIELD,params(Names.REQ_UID))
       
        /* timestamp */
        builder.field(Names.TIMESTAMP_FIELD,timestamp)

	    /* created_at_min */
	    builder.field("created_at_min",params("created_at_min"))

	    /* created_at_max */
	    builder.field("created_at_max",params("created_at_max"))
      
        /* site */
        builder.field(Names.SITE_FIELD,site)
      
        /* user */
        builder.field(Names.USER_FIELD,user)
        
        /* step */
        builder.field(Names.STEP_FIELD,(x._2 + 1))
        
        x._1.foreach(entry => builder.field(entry._1,entry._2))       
        
        builder.endObject()
        builder
        
      })
    
    })
    
  }
  /*
   * The Intent Recognition engine returns a list of Markovian states; the ordering
   * of these states reflects the number of steps looked ahead
   */
  private def buildForecasts(amount:Float,time:Long,states:List[MarkovState]):List[Map[String,Any]] = {
    
    val result = ArrayBuffer.empty[Map[String,Any]]
    val steps = states.size
    
    if (steps == 0) return result.toList
    
    val state = states.head
    /* 
     * The AmountHandler uses the predefined amount horizon to
     * re-interpret the amount sub state
     */    
    val next_amount = StateHandler.nextAmount(state.name, amount)
    /*
     * The AmountHandler uses the predefined time horizon to
     * re-interpret the time sub state; the days period is e.g.
     * 15, 45 or 90 days (from the last purchase)
     */
    val next_days = StateHandler.nextDays(state.name)
    val next_score = state.probability
    
    val source = HashMap.empty[String,Any]
    
    source += Names.AMOUNT_FIELD -> next_amount
    source += Names.DAYS_FIELD -> next_days
    
    source += Names.STATE_FIELD -> state.name
    source += Names.SCORE_FIELD -> next_score
    
    result += source.toMap

    var pre_amount = next_amount
    var pre_score = next_score
    
    var sum_days = next_days
    var sum_amount = next_amount
    
    for (state <- states.tail) {

      val next_days = StateHandler.nextDays(state.name)
      val next_amount = StateHandler.nextAmount(state.name, pre_amount)
      
      /* Conditional probability */
      val next_score = state.probability * pre_score
      /*
       * Add aggregated (sum) amount and days period to 
       * the forecast
       */
      sum_days += next_days
      sum_amount += next_amount

      pre_amount = next_amount
      pre_score = next_score
      
      val source = HashMap.empty[String,Any]
    
      source += Names.AMOUNT_FIELD -> sum_amount.asInstanceOf[Object]
      source += Names.DAYS_FIELD -> sum_days.asInstanceOf[Object]
    
      source += Names.STATE_FIELD -> state.name
      source += Names.SCORE_FIELD -> next_score.asInstanceOf[Object]
      
      result += source.toMap
 
    }
    
    result.toList
    
  }
  
  private def buildRemoteRequest(params:Map[String,String],states:List[String]):(String,String) = {

    val service = "intent"
    val task = "get:state"

    /*
     * The list of last customer purchase states must be added to the 
     * request parameters; note, that this done outside the STMHandler 
     */
    val new_params = Map(Names.REQ_STATES -> states.mkString(",")) ++ params
      
    val data = new STMHandler().get(new_params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  
  private def transform(rawset:List[Order]):List[(String,String,Float,Long,String)] = {
    
    /*
     * Group purchases by site & user and restrict to those
     * users with more than one purchase
     */
    val orders = rawset.map(order => (order.site,order.user,order.timestamp,order.amount))    
    orders.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1
      val orders = p._2.map(x => (x._3,x._4)).toList.sortBy(_._1).reverse.take(2)
      
      val (last_time,last_amount) = orders.head
      val (prev_time,prev_amount) = orders.last
        
      /* 
       * Determine first sub state from amount and second
       * sub state from time elapsed between these orders
       * */
      val astate = StateHandler.stateByAmount(last_amount,prev_amount)
      val tstate = StateHandler.stateByTime(last_time,prev_time)
      
      val last_state = astate + tstate
      (site,user,last_amount,last_time,last_state)
      
    }).toList
    
  }
  
}