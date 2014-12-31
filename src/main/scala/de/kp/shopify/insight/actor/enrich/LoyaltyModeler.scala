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

import de.kp.shopify.insight.actor.BaseActor
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.elastic._

import de.kp.shopify.insight.analytics._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class LoyaltyModeler(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty model building started.""",uid)
       
        /*
         * STEP #1: Transform Shopify orders into sequences of observed states; 
         * these observations are then used to determine the assigned hidden 
         * loyalty states
         */
        val observations = transform(requestCtx.getOrders(req_params))
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Orders successfully transformed into observations.""",uid)

        /*
         * STEP #2: Retrieve hidden Markon states from Intent Recognition engine, 
         * combine those and observations into a user specific loyalty trajectories
         * 
         */
        val (service,req) = buildRemoteRequest(req_params,observations)
        val response = requestCtx.getRemoteContext.send(service,req).mapTo[String]     
        
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of hidden Markov states failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {
            
              val trajectories = toSources(req_params,res,observations)
            
              /*
               * STEP #3: Register the trajectories derived from the hidden state model
               */
              if (requestCtx.putSourcesJSON("orders","loyalty",trajectories) == false)
                throw new Exception("Indexing processing has been stopped due to an internal error.")

              requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "ULOYAM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
            
            }
          
          }

        }
        /*
         * The Intent Recognition engine returned an error message
         */
        response.onFailure {
          case throwable => {
                    
            requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of hidden Markov states failed due to an internal error.""",uid)
           
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
        
      } catch {
        case e:Exception => {
                    
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of hidden Markov states failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }

  }  
  /**
   * Buid user loyalty trajectories
   */
  private def toSources(params:Map[String,String],response:ServiceResponse,observations:List[(String,String,List[String])]):List[XContentBuilder] = {
    
    val states_list = response.data(Names.REQ_RESPONSE).split(";").map(x => x.split(","))    
    val sources = ArrayBuffer.empty[XContentBuilder]
    
    val len = observations.size
    (0 until len).foreach(i => {
      
      val data = new java.util.HashMap[String,Object]()
      
      val (site,user,observation) = observations(i)
      val states = states_list(i)
      /*
       * From the states, we also derive the percentage of the different 
       * loyalty states. Note, that these loyalty rates are the counterpart 
       * to  Sentiment Analysis, when content is considered
       */
      val rates = states.groupBy(x => x).map(x => (x._1, x._2.length.toDouble / states.length))    
    
      val low  = if (rates.contains("L")) rates("L") else 0.0 
      val norm = if (rates.contains("N")) rates("N") else 0.0 

      val high = if (rates.contains("H")) rates("H") else 0.0 
          
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))

	  /* created_at_min */
	  builder.field("created_at_min",params("created_at_min"))

	  /* created_at_max */
	  builder.field("created_at_max",params("created_at_max"))
      
      /* site */
      builder.field(Names.SITE_FIELD,site)
      
      /* user */
      builder.field(Names.USER_FIELD,user)
      
      /* trajectory */
      builder.field(Names.TRAJECTORY_FIELD,states.toList)
      
      /* low */
      builder.field("low",low)
      
      /* norm */
      builder.field("norm",norm)
      
      /* high */
      builder.field("high",high)
      
      builder.endObject()
      sources += builder
      
    })
    
    sources.toList    
  
  }
  
  private def buildRemoteRequest(params:Map[String,String],observations:List[(String,String,List[String])]):(String,String) = {

    val service = "intent"
    val task = "get:state"

    /*
     * The list of last customer observations must be added to the 
     * request parameters; note, that this done outside the HSMHandler 
     */
    val new_params = Map(Names.REQ_OBSERVATIONS -> observations.map(x => x._3.mkString(",")).mkString(";")) ++ params
       
    val data = new HSMHandler().get(new_params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }

  private def transform(rawset:List[Order]):List[(String,String,List[String])] = {

    /*
     * Extract the temporal and monetary dimension from the raw dataset
     */
    val orders = rawset.map(order => (order.site,order.user,order.timestamp,order.amount))    
    /*
     * Group orders by site & user and restrict to those
     * users with more than one purchase
     */
    orders.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1  
      
      /* Compute time ordered list of (timestamp,amount) */
      val user_orders = p._2.map(x => (x._3,x._4)).toList.sortBy(_._1)      
      
      /******************** MONETARY DIMENSION *******************/
      
      /*
       * Compute the amount sub states from the subsequent pairs 
       * of user amounts; the amount handler holds a predefined
       * configuration to map a pair onto a state
       */
      val user_amounts = user_orders.map(_._2)
      val user_amount_states = user_amounts.zip(user_amounts.tail).map(x => StateHandler.stateByAmount(x._2,x._1))
     
      /******************** TEMPORAL DIMENSION *******************/
       
      /*
       * Compute the timespan sub states from the subsequent pairs 
       * of user timestamps; the amount handler holds a predefined
       * configuration to map a pair onto a state
       */
      val user_timestamps = user_orders.map(_._1)
      val user_time_states = user_timestamps.zip(user_timestamps.tail).map(x => StateHandler.stateByTime(x._2,x._1))

     
      /********************* STATES DIMENSION ********************/

      val user_states = user_amount_states.zip(user_time_states).map(x => x._1 + x._2)
      (site,user,user_states)
      
    }).toList
   
  }

}