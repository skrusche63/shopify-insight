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

import de.kp.shopify.insight.actor.{FeedMaster,FindMaster,MasterActor}
import de.kp.shopify.insight.Configuration

import de.kp.shopify.insight.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.shopify.insight.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (heartbeat,time) = Configuration.actor      
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
   * This master actor supports the preparation of an Elasticsearch index for tracking
   * data elements to build a transaction, sequence or othere database
   */
  val indexer = system.actorOf(Props(new MasterActor("IndexMaster")), name="IndexMaster")
  /*
   * This master actor supports data element tracking to build transaction, sequence or
   * other databases in an Elasticsearch index
   */
  val tracker = system.actorOf(Props(new MasterActor("TrackMaster")), name="TrackMaster")
  /*
   * This master actor is responsible for retrieving status informations from the different
   * predictive engines that form Predictiveworks.
   */
  val monitor = system.actorOf(Props(new MasterActor("StatusMaster")), name="StatusMaster")
  /*
   * The master is responsible for the registration of metadata specifications that determine
   * which external data field has to be mapped onto which internal field
   */
  val registrar = system.actorOf(Props(new MasterActor("MetaMaster")), name="MetaMaster")
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
    path("feed" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doFeed(ctx,service,subject)
	    }
	  }
    }  ~ 
    path("get" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * 'index' is part of the administrate interface and prepares 
     * an Elasticsearch index for tracking data elements
     */
    path("index" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * 'register' is part of the administrative interface and registers field
     * specifications for a certain predictive engine; these specifications 
     * determine which (external) field in a data source has to be mapped onto
     * an internal mining field
     */
    path("register" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx,service,subject)
	    }
	  }
    }  ~ 
    /*
     * 'status' is part of the administrative interface and retrieves the
     * current status of a certain data mining or predictive analytics task;
     * 
     * The results is directly retrieved from the invoked predictive engine.
     */
    path("status" / Segment) {engine => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,engine)
	    }
	  }
    }  ~ 
    /*
     * 'track' provides trackable information either as an event 
     * or as a feature; an event refers to a certain 'item', e.g. 
     * an ecommerce product or service , and a feature refers to 
     * a specific dataset
     */
    path("track" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx, service, subject)
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
   * 
   * The data representation depends on the engine selected, e.g. association
   * analysis is based on the identifier of a certain line item, while intent
   * recognition requires the order and so on.
   */
  private def doFeed[T](ctx:RequestContext,engine:String,subject:String) = {
    
    if (Services.isService(engine) && List("order","product").contains(subject)) {
      
      val task = "feed:" + subject
      val request = new ServiceRequest(engine,task,getRequest(ctx))
    
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
          if (result.isInstanceOf[Recommendations]) {
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
   * Indexing is a common administrative task and is provided here to support
   * shopify customers with a one-stop strategy.
   */
  private def doIndex[T](ctx:RequestContext,engine:String,subject:String) = {
    /*
     * It is validated whether the engine specified is available and
     * whether the subject provided is a valid element specification;
     * we do not make cross-reference checks and ensure existing business
     * rules between {engine} and {subject}
     */    
    if (Services.isService(engine) && Elements.isElement(subject)) {
      
      /* Build indexing request */
      val task = "index:" + subject
      val request = new ServiceRequest(engine,task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second

      val response = ask(indexer,request).mapTo[ServiceResponse] 
      ctx.complete(response)
       
    } else {      
      /* do nothing */
    }
    
  }
  
  /**
   * This method registers field spectification for data mining or predictive
   * analytics task for a certain analytics engine; the request is delegated 
   * to the respective engine and the result is directly returned   
   */
  private def doRegister[T](ctx:RequestContext,engine:String,subject:String) = {
    /*
     * It is validated whether the engine specified is available and
     * whether the subject provided is a valid metadata specification;
     * we do not make cross-reference checks and ensure existing business
     * rules between {engine} and {subject}
     */    
    if (Services.isService(engine) && Metadata.isMetadata(subject)) {
      
      /* Build registration request */
      val task = "register:" + subject
      val request = new ServiceRequest(engine,task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second

      val response = ask(registrar,request).mapTo[ServiceResponse] 
      ctx.complete(response)
       
    } else {      
      /* do nothing */
    }
   
  }

  /**
   * This method retrieves the status of a certain data mining or predictive
   * analytics task from a specific predictive analytics engine; the request
   * is delegated to the respective engine and the result is directly returned
   */
  private def doStatus[T](ctx:RequestContext,engine:String) = {
    
    /* Build status request */
    val task = "status"
    val request = new ServiceRequest(engine,task,getRequest(ctx))
    
    implicit val timeout:Timeout = DurationInt(time).second
    /*
     * Invoke monitor actor to retrieve the status information;
     * the result is returned as JSON data structure
     */
    val response = ask(monitor,request).mapTo[ServiceResponse] 
    ctx.complete(response)
      
  }
  /**
   * This method persists single data elements in an Elasticsearch index;
   * these elements form a transaction, sequence or other database, that
   * is used as data source for data mining and predictive analytics tasks
   */
  private def doTrack[T](ctx:RequestContext,engine:String,subject:String) = {
    /*
     * It is validated whether the engine specified is available and
     * whether the subject provided is a valid element specification;
     * we do not make cross-reference checks and ensure existing business
     * rules between {engine} and {subject}
     */    
    if (Services.isService(engine) && Elements.isElement(subject)) {
      
      /* Build track request */
      val task = "track:" + subject
      val request = new ServiceRequest(engine,task,getRequest(ctx))
    
      implicit val timeout:Timeout = DurationInt(time).second

      val response = ask(tracker,request).mapTo[ServiceResponse] 
      ctx.complete(response)
       
    } else {      
      /* do nothing */
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