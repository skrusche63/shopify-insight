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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.rest.RestService

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.Configuration

import de.kp.shopify.insight.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (heartbeat,time) = Configuration.heartbeat      
  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  /*
   * The MessageListener actor is an overall listener that retrieves the error and
   * interim messages from all the other actors
   */
  val listener = system.actorOf(Props(new MessageListener()))
  /*
   * This Feeder actor is responsible for data collection, i.e. data are gathered 
   * from a certain Shopify store, transformed into 'amount' and 'item' specific
   * information elements and then persisted e.g. in an Elasticsearch index, or
   * sent to Kinesis etc
   */
  val feeder = system.actorOf(Props(new Feeder("Feeder",listener,sc)), name="Feeder")
  /*
   * This Preparer actor is responsible for data preparation, i.e. data that have
   * been gathered by the feeder a step before, and for persisting the transformed 
   * data e.g. in an Elasticsearch index, or sent to Kinesis etc
   */
  val preparer = system.actorOf(Props(new Preparer("Preparer",listener,sc)), name="Preparer")
  /*
   * This Monitor actor is responsible for retrieving status informations from the different
   * predictive engines that form Predictiveworks.
   */
  val monitor = system.actorOf(Props(new Monitor("Monitor")), name="Monitor")
  /*
   * This master actor is responsible for initiating data mining or predictive analytics
   * tasks with respect to the different predictive engines
   */
  val trainer = system.actorOf(Props(new Trainer("Trainer",listener,sc)), name="Trainer")
  /*
   * This Finder actor is responsible for retrieving data mining results and predictions
   * from Predictiveworks and merges these results with data from shopify stores
   */
  val finder = system.actorOf(Props(new Finder("Finder",listener)), name="Finder")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {
    /*
     * 'collect' is a shopify specific service, where data from a certain configured
     * shopify store are collected and registered either in an Elasticsearch index
     * or in a cloud database for subsequent data mining and model building.
     * 
     * 'collect' specifies the first step in a pipeline of data analytics steps and
     * turns Shopify orders into 'amount' and 'item' databases
     * 
     * A 'collect' request requires the following parameters:
     * 
     * - site (String)
     * 
     * The 'sink' parameter specifies which data sink has to be used to
     * register 'amount' and 'item' specific datasets
     * 
     * - sink (String)
     * 
     * The subsequent set of parameters is required to retrieve data from 
     * a certain Shopify store
     * 
     * 
     */
    path("collect" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doCollect(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'prepare' supports preparation or preprocessing of 'amount' or 'item' related
     * data. It is the second step in a pipeline of data analytics steps and turns
     * these data into data that are directly accessible by Predictiveworks
     * 
     * A 'prepare' request requires the following parameters:
     * 
     * - site (String)
     * 
     * The parameter values for 'uid' & 'name' are usually system generated values and 
     * must be provided with a 'prepare' request; 'uid' is the unique identifier of a
     * certain data analytics pipeline, and 'name' specifies the unique name of a field
     * or metadata name; note, that models can usually be retrained under the SAME 
     * metadata plan (or name)
     * 
     * - uid (String)
     * - name (String)
     * 
     * The 'source' parameter specifies which data source has to be used to
     * retrieve the 'amount' 
     * 
     * - source (String)
     * 
     * The 'sink' parameter specifies which data sink has to be used to 
     * register the results of the preparation phase
     * 
     * - sink (String)
     * 
     */
    path("prepare" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doPrepare(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'train' starts a certain data mining or model building task and is the
     * third step in a pipeline of data analytics steps.
     * 
     * A 'train' request requires the following parameters:
     * 
     * - site (String)
     * 
     * - uid (String)
     * - name (String)
     * 
     */
    path("train" / Segment) {service => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,service)
	    }
	  }
    }  ~ 
    /*
     * 'status' is part of the administrative interface and retrieves the
     * current status of a certain data mining or model building task
     */
    path("status" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,subject)
	    }
	  }
    }  ~ 
    path("product" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doProduct(ctx,subject)
	    }
	  }
    }
  
  }
  /**
   * 'collect' is an adimistrative task to collect data from a certain
   * shopify store and register them in either an Elasticsearch index or
   * a cloud database
   */
  private def doCollect[T](ctx:RequestContext,subject:String) = {
    
    implicit val timeout:Timeout = DurationInt(time).second
    
    if (List("order","product").contains(subject)) {
      
      val task = "feed:" + subject
      val service = ""

      val params = getRequest(ctx)
      /*
       * The requstor can provide a unique identifier for the data analytics pipeline 
       * that always starts with a 'collect' request; in addition an external 'name' 
       * variable can be provided to distinguish different field or parameter plans;
       * 
       * 'uid' & 'name' are returned with this request and msut be used with follow
       * on requests, such as 'prepare' or 'train' 
       * 
       */
      val uid = if (params.contains(Names.REQ_UID)) params(Names.REQ_UID) 
        else java.util.UUID.randomUUID().toString
      
      val name =  if (params.contains(Names.REQ_NAME)) params(Names.REQ_NAME) 
        else "shopify_" + new java.util.Date().getTime.toString

      val excludes = List(Names.REQ_UID,Names.REQ_NAME)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_UID -> uid,Names.REQ_NAME -> name)

      val request = new ServiceRequest(service,task,data)
      val response = ask(feeder,request).mapTo[ServiceResponse]
      
      ctx.complete(response)
      
    } else {
      /* do nothing */      
    }

  }
  /**
   * 'prepare' is a follow on task that is actually required to transform 'amount' 
   * data into an appropriate state representation, that can directly be analyzed 
   * by Intent Recognition
   */  
  private def doPrepare[T](ctx:RequestContext,subject:String) = {
    
    implicit val timeout:Timeout = DurationInt(time).second
    
    if (List("amount").contains(subject)) {
      
      val task = "prepare:" + subject
      val service = ""

      val data = getRequest(ctx)

      val request = new ServiceRequest(service,task,data)
      val response = ask(preparer,request).mapTo[ServiceResponse]
      
      ctx.complete(response)
      
    } else {
      /* do nothing */      
    }
    
  }

  private def doProduct[T](ctx:RequestContext,subject:String) = {

    implicit val timeout:Timeout = DurationInt(time).second      

    val task = "get:" + subject
    
    val topics = List("cross-sell","loyalty","placement","purchase","recommendation")
    if (topics.contains(subject)) {
      
      val request = new ServiceRequest("",task,getRequest(ctx))
    
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
      
    }
  }
  /**
   * 'status' is an administration request to determine whether a certain data
   * mining task has been finished or not.
   */
  private def doStatus[T](ctx:RequestContext,subject:String) = {
    
    implicit val timeout:Timeout = DurationInt(time).second
    val task = "status:" + subject
    
    val topics = List("latest","all")
    if (topics.contains(subject)) {
        
      val request = new ServiceRequest("",task,getRequest(ctx))
      /*
       * Invoke monitor actor to retrieve the status information;
       * the result is returned as JSON data structure
       */
      val response = ask(monitor,request).mapTo[ServiceResponse] 
      ctx.complete(response)
      
    } else {
      /* do nothing */      
    }
  
  }
  /**
   * This request starts a certain data mining or model building task;
   * actually the following models are supported:
   * 
   * - cross-sell
   * - loyalty
   * - placement
   * - purchase
   * - recommendation
   * 
   */
  private def doTrain[T](ctx:RequestContext,subject:String) = {
    
    implicit val timeout:Timeout = DurationInt(time).second
    val task = "train:" + subject
    
    val topics = List("cross-sell","loyalty","placement","purchase","recommendation")
    if (topics.contains(subject)) {
      
      val request = new ServiceRequest("",task,getRequest(ctx))

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

}