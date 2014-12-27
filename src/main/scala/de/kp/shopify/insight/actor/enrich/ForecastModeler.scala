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

import de.kp.shopify.insight.PrepareContext

import de.kp.shopify.insight.actor._

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.source._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * ForecastModeler is an actor that analyzes Shopify orders of a certain time interval 
 * (last 30 days from now on) and computes from the last two transactions on a per 
 * user basis a forecast of n steps with respect to next amount and datetime
 * 
 * This is part of the 'enrich' sub process that represents the third component of 
 * the data analytics pipeline.
 * 
 */
class ForecastModeler(prepareContext:PrepareContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        prepareContext.listener ! String.format("""[INFO][UID: %s] Purchase forecast model building started.""",uid)
        
        /*
         * STEP #1: Transform Shopify orders into purchases; these purchases are used 
         * to compute n-step ahead forecasts with respect to purchase amount and time
         */
        val purchases = transform(prepareContext.getPurchases(req_params))
      
        prepareContext.listener ! String.format("""[INFO][UID: %s] Orders successfully transformed into purchases.""",uid)

        /*
         * STEP #2: Retrieve Markovian rules from Intent Recognition engine, combine
         * rules and purchases into a user specific set of purchase forecasts
         * 
         */
        val (service,req) = buildRemoteRequest(req_params,purchases.map(_._5))
        val response = prepareContext.getRemoteContext.send(service,req).mapTo[String]     
        
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              prepareContext.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {

              val forecasts = buildForecasts(res,purchases)
            
              val handler = new ElasticHandler()

              if (handler.putSources("orders","forecasts",forecasts) == false)
                throw new Exception("Indexing processing has been stopped due to an internal error.")

              prepareContext.listener ! String.format("""[INFO][UID: %s] Purchase forecast model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "UFORCM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
        
            }
            
          }

        }
        response.onFailure {
          case throwable => {

            prepareContext.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an internal error.""",uid)
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
        
      } catch {
        case e:Exception => {

          prepareContext.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }

  }  

  private def buildForecasts(response:ServiceResponse,purchases:List[(String,String,Float,Long,String)]):List[java.util.Map[String,Object]] = {

    val uid = response.data(Names.REQ_UID)
    
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
        
        val source = new java.util.HashMap[String,Object]()
        
        source += Names.SITE_FIELD -> site
        source += Names.USER_FIELD -> user
        
        source += Names.UID_FIELD -> uid
        source += Names.STEP_FIELD -> (x._2 + 1).asInstanceOf[Object]
        
        x._1.foreach(entry => source += entry._1 -> entry._2)       
        source
        
      })
    
    })
    
  }
  /*
   * The Intent Recognition engine returns a list of Markovian states; the ordering
   * of these states reflects the number of steps looked ahead
   */
  private def buildForecasts(amount:Float,time:Long,states:List[MarkovState]):List[java.util.Map[String,Object]] = {
    
    val result = ArrayBuffer.empty[java.util.Map[String,Object]]
    val steps = states.size
    
    if (steps == 0) return result.toList
    
    val record = states.head
    
    val next_time = AmountHandler.nextDate(record.name, time)
    val next_amount = AmountHandler.nextAmount(record.name, amount)
   
    val source = new java.util.HashMap[String,Object]()
    
    source += Names.AMOUNT_FIELD -> next_amount.asInstanceOf[Object]
    source += Names.TIMESTAMP_FIELD -> next_time.asInstanceOf[Object]
    
    source += Names.SCORE_FIELD -> record.probability.asInstanceOf[Object]
    result += source

    var pre_time = next_time
    var pre_amount = next_amount
    
    for (record <- states.tail) {

      val next_time = AmountHandler.nextDate(record.name, pre_time)
      val next_amount = AmountHandler.nextAmount(record.name, pre_amount)
   
      val source = new java.util.HashMap[String,Object]()
    
      source += Names.AMOUNT_FIELD -> next_amount.asInstanceOf[Object]
      source += Names.TIMESTAMP_FIELD -> next_time.asInstanceOf[Object]
    
      source += Names.SCORE_FIELD -> record.probability.asInstanceOf[Object]
      result += source

      pre_time = next_time
      pre_amount = next_amount
 
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
  
  private def transform(purchases:List[AmountObject]):List[(String,String,Float,Long,String)] = {
    
    /*
     * Group purchases by site & user and restrict to those
     * users with more than one purchase
     */
    val result = purchases.groupBy(p => (p.site,p.user)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1
      val orders = p._2.map(v => (v.timestamp,v.amount)).toList.sortBy(_._1).reverse.take(2)
      
      val (last_time,last_amount) = orders.head
      val (prev_time,prev_amount) = orders.last
        
      /* 
       * Determine first sub state from amount and second
       * sub state from time elapsed between these orders
       * */
      val astate = AmountHandler.stateByAmount(last_amount,prev_amount)
      val tstate = AmountHandler.stateByTime(last_time,prev_time)
      
      val last_state = astate + tstate
      (site,user,last_amount,last_time,last_state)
      
    })
    
    result.toList 
    
  }
  
}