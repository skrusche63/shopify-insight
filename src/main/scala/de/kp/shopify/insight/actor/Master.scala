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

import org.apache.spark.SparkContext
import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.Configuration

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.HashMap

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * The Feeder actor is responsible for retrieving either orders or
 * products from a remote Shopify store (via REST API), transforming
 * these data into e.g. 'amount, 'item, and 'state' object and sending
 * them to a specific data sink for registration.
 */
class Feeder(name:String,listener:ActorRef,@transient sc:SparkContext) extends MasterActor(name) {

  override def execute(req:ServiceRequest):Future[Any] = {

    /*
     * Feeding orders or products from a Shopify store to a certain data
     * endpoint (e.g. an Elasticsearch index) can be longer running
     * task; to this end a fire-and-forget approach is used here.
     */
    val response = try {      
      
      val uid = req.data(Names.REQ_UID)      
      val sink = req.data(Names.REQ_SINK)
      
      sink match {
        
        case Sinks.ELASTIC => {
          /*
           * Send request to ElasticFeeder actor and inform requestor
           * that the tracking process has been started. Error and
           * interim messages of this process are sent to the listener
           */
          val actor = context.actorOf(Props(new ElasticFeeder(listener)))
          
          val reqdata = req.data ++ addParams(req.data)
          actor ! new ServiceRequest("",req.task,reqdata)
          /*
           * This message is sent directly after the request has been
           * delegated to the ElasticFeeder
           */
          val resdata = Map(Names.REQ_MESSAGE -> Messages.TRACKING_STARTED(uid)) ++ req.data   
          new ServiceResponse("insight","collect",resdata,ResponseStatus.SUCCESS)
          
        }
        
        case _ => throw new Exception(String.format("""[UID: %s] The sink '%s' is not supported.""",uid,sink))
      
      }
    
    } catch {
      case e:Exception => failure(req,e.getMessage)

    } 
    
    Future {response}
    
  }
  
  private def addParams(params:Map[String,String]):Map[String,String] = {

    val days = if (params.contains(Names.REQ_DAYS)) params(Names.REQ_DAYS).toInt else 30
    
    val created_max = new DateTime()
    val created_min = created_max.minusDays(days)

    val data = HashMap(
      "created_at_min" -> formatted(created_min.getMillis),
      "created_at_max" -> formatted(created_max.getMillis)
    )
    
    /*
     * We restrict to those orders that have been paid,
     * and that are closed already, as this is the basis
     * for adequate forecasts 
     */
    data += "financial_status" -> "paid"
    data += "status" -> "closed"

    data.toMap
    
  }
  /**
   * This method is used to format a certain timestamp, provided with 
   * a request to collect data from a certain Shopify store
   */
  private def formatted(time:Long):String = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
    
    formatter.print(time)
    
  }
  
}

class Finder(name:String,listener:ActorRef) extends MasterActor(name) {

  override def execute(req:ServiceRequest):Future[Any] = {
    
    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second

    val actor = context.actorOf(Props(new RemoteQuestor(ctx,listener)))
    ask(actor,req)
    
  }
  
}

class Monitor(name:String) extends MasterActor(name) {
  
  override def execute(req:ServiceRequest):Future[Any] = {
    
    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second

    val actor = context.actorOf(Props(new StatusQuestor(Configuration)))
    ask(actor,req)
    
  }
  
}

class Preparer(name:String,listener:ActorRef,@transient sc:SparkContext) extends MasterActor(name) {

  override def execute(req:ServiceRequest):Future[Any] = {

    val response = try {      
      
      val uid  = req.data(Names.REQ_UID)      
      val sink = req.data(Names.REQ_SINK)
      
      sink match {
        
        case Sinks.ELASTIC => {

          val actor = context.actorOf(Props(new ElasticPreparer(sc,listener)))
          actor ! req
          /*
           * This message is sent directly after the request has been
           * delegated to the ElasticPreparer
           */
          val data = Map(Names.REQ_MESSAGE -> Messages.DATA_PREPARATION_STARTED(uid)) ++ req.data   
          new ServiceResponse("insight","prepare",data,ResponseStatus.SUCCESS)
          
        }
        
        case _ => throw new Exception(String.format("""[UID: %s] The sink '%s' is not supported.""",uid,sink))
      
      }
    
    } catch {
      case e:Exception => failure(req,e.getMessage)

    } 
    
    Future {response}
    
  }
  
}

class Trainer(name:String,listener:ActorRef,@transient sc:SparkContext) extends MasterActor(name) {

  override def execute(req:ServiceRequest):Future[Any] = {
    
    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second

    val actor = context.actorOf(Props(new RemoteBuilder(ctx,listener)))
    ask(actor,req)
    
  }
  
}