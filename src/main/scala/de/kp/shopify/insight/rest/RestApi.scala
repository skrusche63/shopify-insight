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
import spray.httpx.encoding.Gzip
import spray.httpx.marshalling.Marshaller

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.EncodingDirectives
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.shopify.insight.actor.{MasterActor,FeedMaster}
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
   * gathered from a certain Shopify store and second indexed in an Elasticsearch index;
   * 
   * note, that this mechanism does not use the 'indexer' as this actor is exclusively responsible
   * for external indexing requests
   */
  val feeder = system.actorOf(Props(new FeedMaster("FeedMaster")), name="FeedMaster")
  
  val finder = system.actorOf(Props(new MasterActor("FindMaster")), name="FindMaster")
  val indexer = system.actorOf(Props(new MasterActor("IndexMaster")), name="IndexMaster")

  val monitor = system.actorOf(Props(new MasterActor("StatusMaster")), name="StatusMaster")
  val registrar = system.actorOf(Props(new MasterActor("MetaMaster")), name="MetaMaster")
  
  val tracker = system.actorOf(Props(new MasterActor("TrackMaster")), name="TrackMaster")
  val trainer = system.actorOf(Props(new MasterActor("TrainMaster")), name="TrainMaster")
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {
    
    path("feed" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doFeed(ctx,subject)
	    }
	  }
    }  ~ 
    path("get" / Segment / Segment) {(service,concept) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,service,concept)
	    }
	  }
    }  ~ 
    path("index" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,service,subject)
	    }
	  }
    }  ~ 
    path("register" / Segment / Segment) {(service,subject) => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx,service,subject)
	    }
	  }
    }  ~ 
    path("status" / Segment) {service => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,service)
	    }
	  }
    }  ~ 
    path("train" / Segment) {service => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,service)
	    }
	  }
    }
  
  }
  private def doFeed[T](ctx:RequestContext,subject:String) = {
    
    val service = "feed"
    subject match {
      /*
       * Feed 'order' data from a certain shopify store and
       * index in an Elasticsearch index
       */
      case "order" => doRequest(ctx,service,"collect:" + subject)
      /*
       * Feed product data from a certain shopify store and
       * index in an Elasticsearch index
       */  
      case "product" => doRequest(ctx,service,"collect:" + subject)
        
      case _  => {/* do nothing */}
    }

  }
  
  /**
   * The PredictiveWorks. client determines whether the provided service 
   * or concept is supported; the REST API is responsible for delegating
   * the request to the respective master actors as fast as possible
   */
  private def doGet[T](ctx:RequestContext,service:String,subject:String) = doRequest(ctx,service,"get:" + subject)
   
  private def doIndex[T](ctx:RequestContext,service:String,subject:String) = {

    val task = "index"    
    service match {

 	  case "association" => {
	    
	    subject match {
          /* ../index/association/item */
	      case "item" => doRequest(ctx,service,task+":item")
          /* ../index/association/rule */
	      case "rule" => doRequest(ctx,service,task+":rule")
	      
	      case _ => {}
	      
	    }
	    
	  }	      
      
      case "intent" => {
	    
	    subject match {	      
	      /* ../index/intent/amount */
	      case "amount" => doRequest(ctx,service,task+":amount")
	      
	      case _ => {}
	      
	    }
      
      }

	  case "series" => {
	    
	    subject match {
          /* ../index/series/item */
	      case "item" => doRequest(ctx,service,task+":item")
          /* ../index/series/rule */
	      case "rule" => doRequest(ctx,service,task+":rule")
	      
	      case _ => {}
	      
	    }
	    
	  }	      

	  case _ => {}
	  
    }
    
  }
  
  private def doRegister[T](ctx:RequestContext,service:String,subject:String) = {

    service match {
      /* ../register/association/field */
	  case "association" => doRequest(ctx,service,"register")	      
	 
      case "intent" => {
	    
	    subject match {	      
	      /* ../register/intent/loyalty */
	      case "loyalty" => doRequest(ctx,"intent","register:loyalty")

	      /* ../register/intent/purchase */
	      case "purchase" => doRequest(ctx,"intent","register:purchase")
	      
	      case _ => {}
	      
	    }
      
      }
      /* ../register/series/field */
	  case "series" => doRequest(ctx,service,"register")	   
	  
	  case _ => {}
	  
    }
    
  }

  private def doTrain[T](ctx:RequestContext,segment:String) = doRequest(ctx,"outlier","train")

  private def doStatus[T](ctx:RequestContext,service:String) = doRequest(ctx,service,"status")
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master(task),request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
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

      case "feed" => feeder

      case "get"   => finder
      case "index" => indexer
      
      case "register" => registrar
      
      case "status" => monitor
      case "train" => trainer
      
      case _ => null
      
    }
  }

}