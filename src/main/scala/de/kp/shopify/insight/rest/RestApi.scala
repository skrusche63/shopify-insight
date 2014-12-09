package de.kp.shopify.insight.rest
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

import java.util.Date

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.core.model._
import de.kp.spark.core.rest.RestService

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.Configuration

import de.kp.shopify.insight.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (heartbeat,time) = Configuration.heartbeat      
  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  /*
   * This master actor is responsible for data collection and indexing, i.e. data are first 
   * gathered from a certain Shopify store and second persistet in an Elasticsearch index;
   * 
   * note, that this mechanism does not use the 'tracker' as this actor is responsible for
   * externally initiated tracking requests
   */
  val feeder = system.actorOf(Props(new FeedMaster("FeedMaster")), name="FeedMaster")
  /*
   * This master actor is responsible for retrieving status informations from the different
   * predictive engines that form Predictiveworks.
   */
  val monitor = system.actorOf(Props(new StatusMaster("StatusMaster")), name="StatusMaster")
  /*
   * This master actor is responsible for initiating data mining or predictive analytics
   * tasks with respect to the different predictive engines
   */
  val trainer = system.actorOf(Props(new MasterActor("TrainMaster")), name="TrainMaster")
  /*
   * This master actor is responsible for retrieving data mining results and predictions
   * and merging these results with data from shopify stores
   */
  val finder = system.actorOf(Props(new FindMaster("FindMaster")), name="FindMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {
    /*
     * 'feed' is a shopify specific service, where data from a certain
     * configured shopify store are collected and registered in an
     * Elasticsearch index
     */
    path("feed" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doFeed(ctx,subject)
	    }
	  }
    }  ~ 
    path("get" / Segment / Segment) {(engine,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,engine,subject)
	    }
	  }
    }  ~ 
    /*
     * 'status' is part of the administrative interface and retrieves the
     * current status of a certain data mining or predictive analytics task
     */
    path("status" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'train' starts a certain data mining or predictive analytics
     * tasks; the request is delegated to the respective engine and 
     * the result is directly returned to the requestor
     */
    path("train" / Segment) {service => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,service)
	    }
	  }
    }
  
  }
  /**
   * Feeding is an administrative task to collect data from a certain
   * shopify store and register them in an Elasticsearch index. 
   */
  private def doFeed[T](ctx:RequestContext,subject:String) = {
    
    if (List("order","product").contains(subject)) {
      
      val task = "feed:" + subject
      val service = ""
        
      val request = new ServiceRequest(service,task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second

      val response = ask(feeder,request).mapTo[ServiceResponse] 
      ctx.complete(response)
      
    } else {
      /* do nothing */      
    }

  }
  
  private def doGet[T](ctx:RequestContext,engine:String,subject:String) = {
    /*
     * It is validated whether the engine specified is available and
     * whether the subject provided is a valid element specification;
     * we do not make cross-reference checks and ensure existing business
     * rules between {engine} and {subject}
     */    
    if (Services.isService(engine) && Tasks.isTask(subject)) {
      
      val task = "get:" + subject
      val request = new ServiceRequest(engine,task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second      
      val response = ask(finder,request)
      
      response.onSuccess {
        case result => {
          
          /* Different response type have to be distinguished */
          if (result.isInstanceOf[CrossSell]) {
            /*
             * A cross sell is retrieved from the Association Analysis 
             * engine in combination with a Shopify request
             */
            ctx.complete(result.asInstanceOf[Placement])
            
          } else if (result.isInstanceOf[Placement]) {
            /*
             * A product placement is retrieved from the Association
             * Analysis engine in combination with a Shopify request
             */
            ctx.complete(result.asInstanceOf[Placement])
            
          } else if (result.isInstanceOf[Recommendations]) {
             /*
             * Product recommendations is retrieved e.g. from the 
             * Association Analysis engine in combination with a 
             * Shopify request
             */
           ctx.complete(result.asInstanceOf[Recommendations])
            
          } else if (result.isInstanceOf[ServiceResponse]) {
            /*
             * This is the common response type used for almost
             * all requests
             */
            ctx.complete(result.asInstanceOf[ServiceResponse])
            
          }
          
        }
      
      }

      response.onFailure {
        case throwable => ctx.complete(throwable.getMessage)
      }
      
    } else {
      /* do nothing */      
    }

  }
  /**
   * 'status' is an administration request to determine whether a certain data
   * mining task has been finished or not.
   */
  private def doStatus[T](ctx:RequestContext,subject:String) = {
    
    val topics = List("latest","all")
    if (topics.contains(subject)) {
      
      val task = "status" + ":" + subject
      val service = ""
        
      val request = new ServiceRequest("",task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second
      /*
       * Invoke monitor actor to retrieve the status information;
       * the result is returned as JSON data structure
       */
      val response = ask(monitor,request).mapTo[ServiceResponse] 
      ctx.complete(response)

    }
  
  }
  /**
   * This request starts a certain data mining or predictive analytics
   * task for a specific predictive engine; the response is directly
   * returned to the requestor
   */
  private def doTrain[T](ctx:RequestContext,engine:String) = {
    
    if (Services.isService(engine)) {
      
      /* Build train request */
      val task = "train"
      val request = new ServiceRequest(engine,task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second

      val response = ask(trainer,request).mapTo[ServiceResponse] 
      ctx.complete(response)
       
    } else {      
      /* do nothing */
    }
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }
  
  private def master(task:String):ActorRef = {
    
    val req = task.split(":")(0)   
    req match {
      case "get"   => finder
      case _ => null
      
    }
  }

}