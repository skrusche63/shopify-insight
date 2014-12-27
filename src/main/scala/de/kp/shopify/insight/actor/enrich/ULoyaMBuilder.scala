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
import de.kp.shopify.insight.actor.BaseActor

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.source._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class ULoyaMBuilder(serverContext:PrepareContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {
      
      try {
      
        val params = message.data
        val uid = params(Names.REQ_UID)
        
        /*
         * STEP #1: Transform Shopify orders into sequences of observed states; 
         * these observations are then used to determine the assigned hidden 
         * loyalty states
         */
        val observations = transform(serverContext.getPurchases(params))
        /*
         * STEP #2: Retrieve hidden Markon states from Intent Recognition engine, 
         * combine those and observations into a user specific loyalty trajectories
         * 
         */
        val (service,req) = buildRemoteRequest(params,observations)
        val response = serverContext.getRemoteContext.send(service,req).mapTo[String]     
        
        response.onSuccess {
        
          case result => {
 
            val intermediate = Serializer.deserializeResponse(result)
            val trajectories = buildTrajectories(intermediate,observations)
            
            /*
             * STEP #3: Create search index (if not already present);
             * the index is used to register the trajectories derived 
             * from the hideen state model
             */
            val handler = new ElasticHandler()
            
            if (handler.createIndex(params,"orders","loyalty","loyalty") == false)
              throw new Exception("Indexing has been stopped due to an internal error.")
 
            serverContext.listener ! String.format("""[UID: %s] Elasticsearch index created.""",uid)

            if (handler.putLoyalty("orders","loyalty",trajectories) == false)
              throw new Exception("Indexing processing has been stopped due to an internal error.")

            serverContext.listener ! String.format("""[UID: %s] Loyalty perspective registered in Elasticsearch index.""",uid)

            val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PLoyaM")            
            context.parent ! EnrichFinished(data)           
            
            context.stop(self)
        
          }

        }
        /*
         * The Intent Recognition engine returned an error message
         */
        response.onFailure {
          case throwable => {
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
        
      } catch {
        case e:Exception => {
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }

  }  
  private def buildTrajectories(response:ServiceResponse,observations:List[(String,String,List[String])]):List[java.util.Map[String,Object]] = {
   
    val states_list = response.data(Names.REQ_RESPONSE).split(";").map(x => x.split(","))
    val trajectories = ArrayBuffer.empty[java.util.Map[String,Object]]
    
    val len = observations.size
    (0 until len).foreach(i => {
      
      val data = new java.util.HashMap[String,Object]()
      
      val (site,user,observation) = observations(i)
      val states = states_list(i)
      
      data += Names.SITE_FIELD -> site
      data += Names.USER_FIELD -> user
      
      data += Names.TRAJECTORY_FIELD -> states.toList.asInstanceOf[Object]
      
      trajectories += data
      
    })
    
    trajectories.toList    
  
  }
  
  private def buildRemoteRequest(params:Map[String,String],observations:List[(String,String,List[String])]):(String,String) = {

    val service = "intent"
    val task = "get:state"

    // TODO
      
    val data = new HSMHandler().get(params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }

  private def transform(purchases:List[AmountObject]):List[(String,String,List[String])] = {
        
    /*
     * Group purchases by site & user and restrict to those
     * users with more than one purchase
     */
    purchases.groupBy(p => (p.site,p.user)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1
      val orders      = p._2.map(v => (v.timestamp,v.amount)).toList.sortBy(_._1)
      
      /* Extract first order */
      var (pre_time,pre_amount) = orders.head
          
      val states = ArrayBuffer.empty[String]
      for ((time,amount) <- orders.tail) {
        
        /* Determine state from amount */
        val astate = AmountHandler.stateByAmount(amount,pre_amount)
     
        /* Determine state from time elapsed between
         * subsequent orders or transactions
         */
        val tstate = AmountHandler.stateByTime(time,pre_time)
      
        val state = astate + tstate
        states += state
        
        pre_amount = amount
        pre_time   = time
        
      }
      
      val observations = states.toList
      (site,user,observations)
      
    
    }).toList
   
  }

}