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
     * 'product' requests focus on product specific questions
     */
    path("product" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doProduct(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'user' requests focus on user specific questions
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
   * 'collect' describes the starting point of a data analytics process that collects orders
   * or products from a Shopify store through the respective REST API and builds predictive 
   * models to support product cross sell, purchase forecast, product recommendations and more.
   */
  private def doCollect[T](ctx:RequestContext,subject:String) = {
    
    if (List("order","product").contains(subject)) {
      /*
       * A 'collect' request starts a data processing pipeline and is accompanied 
       * by the Pipeliner actor that is responsible for controlling the analytics 
       * pipeline
       */
      val pipeline = system.actorOf(Props(new Pipeliner(sc,listener)))

      val params = getRequest(ctx)
      val uid = java.util.UUID.randomUUID().toString
      /*
       * 'uid', 'name' and 'topic' is set internally and MUST be excluded
       * from the external request parameters
       */
      val excludes = List(Names.REQ_UID,Names.REQ_NAME,Names.REQ_TOPIC)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_UID -> uid,Names.REQ_TOPIC -> subject)

      /* Delegate data collection to Pipeliner */
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

    implicit val timeout:Timeout = DurationInt(time).second      

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

    implicit val timeout:Timeout = DurationInt(time).second      

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