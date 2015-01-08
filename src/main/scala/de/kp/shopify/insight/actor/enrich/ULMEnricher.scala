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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.actor.BaseActor
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.analytics.StateHandler

class ULMEnricher(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty model building started.""",uid)
        
        /*
         * STEP #1: Transform Shopify orders into purchases; these purchases are used 
         * to compute n-step ahead forecasts with respect to purchase amount and time
         */
        val store = String.format("""%s/STM/%s""",requestCtx.getBase,uid)         
        val parquetStates = readParquetStates(store)
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Parquet states successfully retrieved.""",uid)

        /*
         * STEP #2: Retrieve hidden Markon states from Intent Recognition engine, 
         * combine those and observations into a user specific loyalty trajectories
         * 
         */
        val observations = parquetStates.map(_._3).collect.distinct
        val lookup = observations.zipWithIndex.toMap
        
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
 
              val sc = requestCtx.sparkContext
              val table = buildTable(res,parquetStates,lookup)
        
              val sqlCtx = new SQLContext(sc)
              import sqlCtx.createSchemaRDD

              /* 
               * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
               * allowing it to be stored using Parquet. 
               */
              val store = String.format("""%s/ULM/%s""",requestCtx.getBase,uid)         
              table.saveAsParquetFile(store)

              requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "ULM")            
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
  private def buildTable(response:ServiceResponse,parquetStates:RDD[(String,String,String)],lookup:Map[String,Int]):RDD[ParquetULM] = {
    
    val sc = requestCtx.sparkContext
    val states = response.data(Names.REQ_RESPONSE).split(";").zipWithIndex.map(x => (x._2,x._1)).toMap    
    
    val observ_lookup = sc.broadcast(lookup)
    val states_lookup = sc.broadcast(states)
   
    parquetStates.map(o => {
      
      val (site,user,observation) = o
      /* Determine position from observation */
      val pos = observ_lookup.value(observation)
      
      val trajectory = states_lookup.value(pos).split(",") 
      /*
       * From the states, we also derive the percentage of the different 
       * loyalty states. Note, that these loyalty rates are the counterpart 
       * to  Sentiment Analysis, when content is considered
       */
      val rates = trajectory.groupBy(x => x).map(x => (x._1, x._2.length.toDouble / trajectory.length))    
    
      val low  = if (rates.contains("L")) rates("L") else 0.0 
      val norm = if (rates.contains("N")) rates("N") else 0.0 

      val high = if (rates.contains("H")) rates("H") else 0.0 
      
      /*
       * From the loyalty states build an overall score, where "L" = 0,
       * "N" = 1 and "H" = 2
       */
      val ratio = states.map(x => if (x._2 == "L") 0 else if (x._2 == "N") 1 else 2).sum.toDouble / (2 * states.size).toDouble
      val rating = Math.round(5 * ratio).toInt
      
      ParquetULM(site,user,trajectory,low,norm,high,rating)
     
    })
  
  }
  
  private def buildRemoteRequest(params:Map[String,String],observations:Array[String]):(String,String) = {

    val service = "intent"
    val task = "get:state"

    /*
     * The list of last customer observations must be added to the 
     * request parameters; note, that this done outside the HSMHandler 
     */
    val new_params = Map(Names.REQ_OBSERVATIONS -> observations.mkString(";")) ++ params
       
    val data = new HSMHandler().get(new_params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  /**
   * This method reads the Parquet file that specifies the state representation 
   * for all customers that have purchased at least twice since the start of the 
   * collection of the Shopify orders.
   */
  private def readParquetStates(store:String):RDD[(String,String,String)] = {

    val sqlCtx = new SQLContext(requestCtx.sparkContext)
    import sqlCtx.createSchemaRDD
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlCtx.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    val rawset = parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val amount = data("amount").asInstanceOf[Float]
      val timestamp = data("timestamp").asInstanceOf[Long]
      
      (site,user,amount,timestamp)
      
    })
    
    rawset.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1  
      
      /* Compute time ordered list of (amount,timestamp) */
      val user_orders = p._2.map(x => (x._3,x._4)).toList.sortBy(_._2)      
      
      /******************** MONETARY DIMENSION *******************/
      
      /*
       * Compute the amount sub states from the subsequent pairs 
       * of user amounts; the amount handler holds a predefined
       * configuration to map a pair onto a state
       */
      val user_amounts = user_orders.map(_._1)
      val user_amount_states = user_amounts.zip(user_amounts.tail).map(x => StateHandler.stateByAmount(x._2,x._1))
     
      /******************** TEMPORAL DIMENSION *******************/
       
      /*
       * Compute the timespan sub states from the subsequent pairs 
       * of user timestamps; the amount handler holds a predefined
       * configuration to map a pair onto a state
       */
      val user_timestamps = user_orders.map(_._2)
      val user_time_states = user_timestamps.zip(user_timestamps.tail).map(x => StateHandler.stateByTime(x._2,x._1))

     
      /********************* STATES DIMENSION ********************/

      val observation = user_amount_states.zip(user_time_states).map(x => x._1 + x._2).mkString(",")
      (site,user,observation)
      
    })

  }

}