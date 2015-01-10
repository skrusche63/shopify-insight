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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import scala.collection.mutable.ArrayBuffer

private case class MarkovStep(step:Int,amount:Double,time:Long,state:String,score:Double)
/**
 * The UFMEnricher is responsible for building the customer-specific
 * purchase forecast model.
 */
class UFMEnricher(requestCtx:RequestContext) extends BaseActor(requestCtx) {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
        
  import sqlc.createSchemaRDD

  override def receive = {
   
    case message:StartEnrich => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Purchase forecast model building started.""",uid)
        
        /*
         * STEP #1: Load Parquet file with previously build customer state description
         */
        val store = String.format("""%s/STM/%s""",requestCtx.getBase,uid)         
        val parquetFile = readParquetStates(store)
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Parquet states successfully retrieved.""",uid)

        /*
         * Retrieve list of distinct states from user specific parquet states 
         */
        val states = parquetFile.map(x => x.state).collect.distinct
        /*
         * STEP #2: Retrieve Markovian rules from Intent Recognition engine, combine
         * rules and purchases into a user specific set of purchase forecasts
         * 
         */
        val (service,req) = buildRemoteRequest(req_params,states)
        val response = requestCtx.getRemoteContext.send(service,req).mapTo[String]     
        
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Markovian rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {

              val table = buildTable(res,parquetFile)

              /* 
               * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
               * allowing it to be stored using Parquet. 
               */
              val store = String.format("""%s/UFM/%s""",requestCtx.getBase,uid)         
              table.saveAsParquetFile(store)

              requestCtx.listener ! String.format("""[INFO][UID: %s] Purchase forecast model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "UFM")            
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

  private def buildTable(response:ServiceResponse,parquetStates:RDD[ParquetSTM]):RDD[ParquetUFM] = {
    
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
    val lookup = requestCtx.sparkContext.broadcast(rules.items.map(rule => (rule.antecedent,rule.consequent)).toMap)
    parquetStates.flatMap(x => {
      
      val forecasts = buildForecasts(x,lookup.value(x.state))
      forecasts.map(v => ParquetUFM(x.site,x.user,v.step,v.amount,v.time,v.state,v.score))
    
    })
    
  }
  /*
   * The Intent Recognition engine returns a list of Markovian states; the ordering
   * of these states reflects the number of steps looked ahead
   */
  private def buildForecasts(state:ParquetSTM,states:List[MarkovState]):List[MarkovStep] = {
   
    val result = ArrayBuffer.empty[MarkovStep]
    val steps = states.size
    
    if (steps == 0) return result.toList
    
    /*
     * Retrieve quantile boundaries
     */
    val r_b1 = state.r_b1
    val r_b2 = state.r_b2
    val r_b3 = state.r_b3
    val r_b4 = state.r_b4
    val r_b5 = state.r_b5

    val s_b1 = state.s_b1
    val s_b2 = state.s_b2
    val s_b3 = state.s_b3
    val s_b4 = state.s_b4
    val s_b5 = state.s_b5
    
    (0 until steps).foreach(i => {
      
      val markovState = states(i)
        
      /* 
       * A MarkovState is a string that describes an amount
       * and timespan representation in terms of integers
       * from 1..5
       */
      val astate = markovState.name(0).toInt
      val sstate = markovState.name(1).toInt

      val ratio = (
        if (astate == 1) (r_b1 + 0) * 0.5
        else if (astate == 2) (r_b2 + r_b1) * 0.5
        else if (astate == 3) (r_b3 + r_b2) * 0.5
        else if (astate == 4) (r_b4 + r_b3) * 0.5
        else (r_b5 + r_b4) * 0.5
      )

      val span = (
        if (sstate == 1) (s_b5 + s_b4) * 0.5
        else if (sstate == 2) (s_b4 + s_b3) * 0.5
        else if (sstate == 3) (s_b3 + s_b2) * 0.5
        else if (sstate == 4) (s_b2 + s_b1) * 0.5
        else (s_b1 + 0) * 0.5
      )

      if (i == 0) {
        
        val next_amount = state.amount * ratio
        val next_time = Math.round(span) * DAY + state.timestamp
        
        result += MarkovStep(i+1,next_amount,next_time,markovState.name,markovState.probability)
      
      } else {
        
        val previousStep = result(i-1)
        
        val next_amount = previousStep.amount * ratio
        val next_time   = Math.round(span) * DAY + previousStep.time
        
        result += MarkovStep(i+1,next_amount,next_time,markovState.name,markovState.probability)
        
      }
      
      
    })
    
    result.toList
    
  }
  
  private def buildRemoteRequest(params:Map[String,String],states:Array[String]):(String,String) = {

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

  /**
   * This method reads the Parquet file that specifies the state representation 
   * for all customers that have purchased at least twice since the start of the 
   * collection of the Shopify orders.
   */
  private def readParquetStates(store:String):RDD[ParquetSTM] = {
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
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

      val amount = data("amount").asInstanceOf[Double]
      val timestamp = data("timestamp").asInstanceOf[Long]
      
      /*
       * Quantile boundaries for the amount ratio
       * and timespan with respect to subsequent
       * purchase transactions
       */
      val r_b1 = data("r_b1").asInstanceOf[Double]
      val r_b2 = data("r_b2").asInstanceOf[Double]
      val r_b3 = data("r_b3").asInstanceOf[Double]
      val r_b4 = data("r_b4").asInstanceOf[Double]
      val r_b5 = data("r_b5").asInstanceOf[Double]

      val s_b1 = data("s_b1").asInstanceOf[Double]
      val s_b2 = data("s_b2").asInstanceOf[Double]
      val s_b3 = data("s_b3").asInstanceOf[Double]
      val s_b4 = data("s_b4").asInstanceOf[Double]
      val s_b5 = data("s_b5").asInstanceOf[Double]

      val state = data("state").asInstanceOf[String]
            
      ParquetSTM(
          site,
          user,
          amount,
          timestamp,
          r_b1,
          r_b2,
          r_b3,
          r_b4,
          r_b5,
          s_b1,
          s_b2,
          s_b3,
          s_b4,
          s_b5,
          state
      )
      
    })
    
    rawset.groupBy(x => (x.site,x.user)).filter(_._2.size > 1).map(x => {
      x._2.toSeq.sortBy(_.timestamp).last      
    })

  }
}