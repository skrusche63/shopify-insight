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

import scala.concurrent.Future

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.{Duration,DurationInt}

import scala.util.parsing.json._

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.rest.RestService

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.ServerContext

import de.kp.shopify.insight.model._
/**
 * Shopify Insight is a REST based data analytics service, using order data 
 * from the past 30, 60 or 90 days. This service generates multiple insight
 * models and provides them in terms of a set of Elasticsearch indexes.
 * 
 * Elasticsearch is used as the serving layer for all the data analystics
 * results.
 */
class RestApi(host:String,port:Int,system:ActorSystem,@transient sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  private val RouteCache = CachingDirectives.routeCache(1000,16,Duration.Inf,Duration("30 min"))
  
  /*
   * The listener actor is an overall listener that retrieves the error and
   * interim messages from all the other actors
   */
  private val listener = system.actorOf(Props(new MessageListener()))
  private val serverContext = new ServerContext(sc,listener)
  
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {
    /*
     * A 'prepare' request supports the generation of multiple insight models 
     * from orders in a Shopify store; from orders of a defined period of days 
     * (e.g. the last 30, 60 or 90 days), multiple Elasticsearch indexes are
     * built, machine learning models derived by invoking the Association Analysis
     * and Intent Recognition engine of Predictiveworks. 
     * 
     * 'prepare' also specifies the first step in a pipeline of data analytics 
     * pipeline. A 'prepare' request requires the following parameters:
     * 
     * - days (Integer, optional)
     * 
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
     * 'product' requests focus on product specific questions. A 'product' request
     * requires the following parameters:
     * 
     * - uid (String, mandatory)
     * 
     */
    path("product" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doProduct(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'user' requests focus on user specific questions. A 'user' request requires
     * the following parameters:
     * 
     * - uid (String, mandatory)
     * 
     */
    path("user" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doUser(ctx,subject)
	    }
	  }
    }
  
  }
  /**
   * 'prepare' describes the starting point of a data analytics process that collects orders
   * or products from a Shopify store through the respective REST API and builds predictive 
   * models to support product cross sell, purchase forecast, product recommendations and more.
   */
  private def doPrepare[T](ctx:RequestContext,subject:String) = {
    
    if (List("order","product").contains(subject)) {
      /*
       * A 'collect' request starts a data processing pipeline and is accompanied 
       * by the DataPipeline actor that is responsible for controlling the analytics 
       * pipeline
       */
      val pipeline = system.actorOf(Props(new DataPipeline(serverContext)))

      val params = getRequest(ctx)
      val uid = java.util.UUID.randomUUID().toString
      /*
       * 'uid', 'name', 'topic' and 'sink' is set internally and MUST be excluded
       * from the external request parameters
       */
      val excludes = List(Names.REQ_UID,Names.REQ_NAME,Names.REQ_SINK,Names.REQ_TOPIC)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ 
        Map(Names.REQ_UID -> uid,Names.REQ_SINK -> Sinks.ELASTIC,Names.REQ_TOPIC -> subject)

      /* 
       * Delegate data collection to the DataPipeline actor. Note, that this actor is
       * created for each 'prepare' request and stops itself either after having 
       * executed all data processing tasks or after having detected a processing
       * failure.
       */
      pipeline ! StartPipeline(data)

      val message = "Data analytics started."
      ctx.complete(SimpleResponse(uid,message))
      
    } else {
      
      val message = "This request is not supported."
      ctx.complete(SimpleResponse("",message))
           
    }

  }
  /**
   * 'product' supports retrieval of product related mining and prediction
   * results, such as 'collection','cross-sell', 'promotion':
   * 
   * 'collection','cross-sell','promotion'
   * 
   * Shopify supports custom collections, i.e. grouping of products that a 
   * shop owner can create to make their shops easier to browse. 
   * 
   * a) 'collection' suggests products that are often purchased together
   * 
   * b) 'cross-sell' helps to find additional or related products to put into 
   * a collection e.g. mac book related, starting from a set of selected products. 
   * 
   * c) 'promotion' is similar to 'cross-sell' and answers the questions, which
   * products to put into a certain collection to push sale for a list of 
   * selected products.
   * 
   * 'product' requests are completely independent of a certain user and focus
   * on relations between different products. A software product on top of these
   * requests may be "Smart Collection Builder" that suggests products to put
   * into a collection.
   * 
   */
  private def doProduct[T](ctx:RequestContext,subject:String) = {

    implicit val timeout:Timeout = DurationInt(serverContext.getTimeout).second      

    val task = "get:" + subject
    
    val topics = List("collection","cross-sell","promotion")
    if (topics.contains(subject)) {
      
      val request = new ServiceRequest("",task,getRequest(ctx))
    
      // TODO
      
      val response:Future[Any] = null     
      response.onSuccess {
        case result => {

          /* Different response type have to be distinguished */
          if (result.isInstanceOf[Suggestions]) {
            /*
             * Suggestions is a list of product suggestions that
             * can be used Shopify users to build custom collections;
             * this result is provided by 'collection' requests
             */
            ctx.complete(result.asInstanceOf[Suggestions])
            
          } else if (result.isInstanceOf[Products]) {
            /*
             * Products is a list of Shhopify products that are
             * related to set of selected products; this result
             * is provided by 'cross-sell' & 'promotion' requests
             */
            ctx.complete(result.asInstanceOf[Products])
            
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
  
  private def doUser[T](ctx:RequestContext,subject:String) = {

    implicit val timeout:Timeout = DurationInt(serverContext.getTimeout).second      

    val task = "get:" + subject
    
    val topics = List("forecast","loyalty","recommendation")
    if (topics.contains(subject)) {
      
      val request = new ServiceRequest("",task,getRequest(ctx))
    
      // TODO
      
      val response:Future[Any] = null     
      response.onSuccess {
        case result => {
          
          if (result.isInstanceOf[UserForecasts]) {
            /*
             * A user forecast is retrieved from the Intent Recognition
             * engine in combination with a Shopify request
             */
            ctx.complete(result.asInstanceOf[UserForecasts])
            
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