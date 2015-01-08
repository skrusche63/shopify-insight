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

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.io._
import de.kp.shopify.insight.analytics.StateHandler

import scala.collection.mutable.ArrayBuffer

private case class MarkovStep(step:Int,amount:Float,time:Long,state:String,score:Double)
/**
 * The UFMEnricher is responsible for building the customer-specific
 * purchase forecast model.
 */
class UFMEnricher(requestCtx:RequestContext) extends BaseActor {

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
        val parquetStates = readParquetStates(store)
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Parquet states successfully retrieved.""",uid)

        /*
         * Retrieve list of distinct states from user specific parquet states 
         */
        val states = parquetStates.map(x => x._5).collect.distinct
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

              val sc = requestCtx.sparkContext
              val table = buildTable(res,parquetStates)
        
              val sqlCtx = new SQLContext(sc)
              import sqlCtx.createSchemaRDD

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

  private def buildTable(response:ServiceResponse,parquetStates:RDD[(String,String,Float,Long,String)]):RDD[ParquetUFM] = {
    
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
    
    /*
     * Compute next probable purchase amount and time by combining the Markovian
     * rules and the purchases retrieved from the Shopify store
     */
    parquetStates.flatMap(p => {
      
      val (site,user,amount,time,state) = p
      
      val forecasts = buildForecasts(amount,time,lookup.value(state))
      forecasts.map(x => ParquetUFM(site,user,x.step,x.amount,x.time,x.state,x.score))
    
    })
    
  }
  /*
   * The Intent Recognition engine returns a list of Markovian states; the ordering
   * of these states reflects the number of steps looked ahead
   */
  private def buildForecasts(amount:Float,time:Long,states:List[MarkovState]):List[MarkovStep] = {
   
    val result = ArrayBuffer.empty[MarkovStep]
    val steps = states.size
    
    if (steps == 0) return result.toList
    
    (0 until steps).foreach(i => {
      
      val markovState = states(i)
      if (i == 0) {
        
        val next_amount = StateHandler.nextAmount(markovState.name, amount)
        val next_time   = StateHandler.nextDate(markovState.name, time)
        
        result += MarkovStep(i+1,next_amount,next_time,markovState.name,markovState.probability)
      
      } else {
        
        val previousStep = result(i-1)
        
        val next_amount = StateHandler.nextAmount(markovState.name, previousStep.amount)
        val next_time   = StateHandler.nextDate(markovState.name, previousStep.time)
        
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
  private def readParquetStates(store:String):RDD[(String,String,Float,Long,String)] = {

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
      /*
       * Constrain to the last two transactions and retrieve
       * the respective timestamp and amount
       */
      val orders = p._2.toSeq.sortBy(_._4).reverse.take(2)
      
      val (last_amount,last_time) = (orders.head._3,orders.head._4)
      val (prev_amount,prev_time) = (orders.last._3,orders.last._4)
        
      /* 
       * Determine first sub state from amount and second
       * sub state from time elapsed between these orders
       * */
      val astate = StateHandler.stateByAmount(last_amount,prev_amount)
      val tstate = StateHandler.stateByTime(last_time,prev_time)
      
      val last_state = astate + tstate
      (site,user,last_amount,last_time,last_state)
      
    })

  }
}