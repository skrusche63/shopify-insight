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

import de.kp.shopify.insight.{RequestContext => RequestCtx}
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
  private val requestCtx = new RequestCtx(sc,listener)
  
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
     * A 'synchronize' request supports the creation or update of the customer and 
     * product database as respective Elasticsearch indexes, and represents copies
     * of the customer and product data of a certain Shopify store
     */
    path("synchronize" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doSynchronize(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'aggregate' requests focus on the aggregated or summary day that have been
     * extracted from orders or purchase transactions of a certain time span.
     * 
     * The following data have been extracted and are made available through this
     * request. Note, that these data refer to all orders and customers:
     * 
     * CUSTOMER:
     * 
     * tbd
     * 
     * ORDER:
     * 
     * - total number of orders
     * 
     * a) monetary dimension
     * 
     * - total amount of money spent 
     * 
     * - average amount of money spent
     * 
     * - minimum & maximum amount of money spent
     * 
     * b) temporal dimension
     * 
     * - average time elapsed between two subsequent 
     *   transactions
     * 
     * - minimum & maximum time elpased between to subsequent
     *   transactions
     *   
     * - support & preference for the day of the week (note, that
     *   only those days are avaiable where transactions have been
     *   made  
     *   
     * - support & preference for the time of the day, where a day
     *   is separated into 4 different time areas
     *   
     * - support & preference for the products purchased 
     * 
     * 
     * The request requires the following paramaters:
     * 
     * - uid (String, mandatory)
     */
    path("aggregate" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doAggregate(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * The following parameters are required:
     * 
     * - uid (String, mandatory)
     * 
     */
    path("forecast") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doForecast(ctx)
	    }
	  }
    }  ~ 
    /*
     * The following parameters are required:
     * 
     * - uid (String, mandatory)
     * 
     */
    path("loyalty") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doLoyalty(ctx)
	    }
	  }
    }  ~ 
    /*
     * The following parameters are required:
     * 
     * - uid (String, mandatory)
     * 
     */
    path("recommendation") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRecommendation(ctx)
	    }
	  }
    }  ~ 
    path("product" / Segment) {method => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doProduct(ctx,method)
	    }
	  }
    }  ~ 
    path("user" / Segment) {method => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doUser(ctx,method)
	    }
	  }
    }  ~ 
    /*
     * 'task' requests focus on the registered tasks, i.e. either
     * 'prepare' or 'synchronize' tasks; this request is necessary
     * to retrieve the 'uid' of a certain task
     * 
     * The following parameters are required:
     * 
     * - uid (String, mandatory)
     */
    path("task") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTask(ctx)
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
       * A 'prepare' request starts a data processing pipeline and is accompanied 
       * by the DataPipeline actor that is responsible for controlling the analytics 
       * pipeline
       */
      val pipeline = system.actorOf(Props(new DataPipeline(requestCtx)))

      val params = getRequest(ctx)
      val uid = java.util.UUID.randomUUID().toString
      /*
       * 'uid', 'name' and 'topic' is set internally and MUST be excluded
       * from the external request parameters
       */
      val excludes = List(Names.REQ_UID,Names.REQ_NAME,Names.REQ_TOPIC)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ 
        Map(Names.REQ_UID -> uid,Names.REQ_TOPIC -> subject)

      /* 
       * Delegate data preparation and model building to the DataPipeline actor. Note, that 
       * this actor is created for each 'prepare' request and stops itself either after having 
       * executed all data processing tasks or after having detected a processing failure.
       */
      pipeline ! StartPipeline(data)

      val message = "Data analytics started."
      ctx.complete(SimpleResponse(uid,message))
      
    } else {
      
      val message = "This request is not supported."
      ctx.complete(SimpleResponse("",message))
           
    }

  }
  private def doSynchronize[T](ctx:RequestContext,subject:String) = {
    
    if (List("customer","product").contains(subject)) {
      /*
       * A 'synchronize' request starts a processing pipeline to synchronize 
       * the customer and product database of a Shopify store with an external
       * Elasticsearch cluster
       */
      val pipeline = system.actorOf(Props(new SyncPipeline(requestCtx)))

      val params = getRequest(ctx)
      val uid = java.util.UUID.randomUUID().toString
      /*
       * 'uid' and 'topic' is set internally and MUST be excluded
       * from the external request parameters
       */
      val excludes = List(Names.REQ_UID,Names.REQ_TOPIC)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ 
        Map(Names.REQ_UID -> uid,Names.REQ_TOPIC -> subject)

      /* 
       * Delegate database synchronization to the SyncPipeline actor.
       */
      pipeline ! StartPipeline(data)

      val message = "Database synchronization started."
      ctx.complete(SimpleResponse(uid,message))
      
    } else {
      
      val message = "This request is not supported."
      ctx.complete(SimpleResponse("",message))
           
    }

  }
  /**
   * An 'aggregate' request supports the retrieval of the aggregated data,
   * extracted from all orders or purchase transactions of the last n days
   */
  private def doAggregate[T](ctx:RequestContext,subject:String) = {
    
    if (List("customer","order").contains(subject)) {
    
      implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
      val actor = system.actorOf(Props(new query.AggregateQuestor(requestCtx)))
      val params = getRequest(ctx)
      
      val excludes = List(Names.REQ_TOPIC)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_TOPIC -> subject)
    
      val request = AggregateQuery(data)
      val response = ask(actor,request)     

      response.onSuccess {
        
        case result => {

          if (result.isInstanceOf[InsightAggregate]) {
            ctx.complete(result.asInstanceOf[InsightAggregate])
            
          } else if (result.isInstanceOf[SimpleResponse]) {
            ctx.complete(result.asInstanceOf[SimpleResponse])
            
          } else {
            /* do nothing */
          }
          
        }
      
      }

      response.onFailure {
        case throwable => ctx.complete(throwable.getMessage)
      }
      
    }
  
  }
  
  private def doForecast[T](ctx:RequestContext) = {

    implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
    val actor = system.actorOf(Props(new query.ForecastQuestor(requestCtx)))
    val params = getRequest(ctx)
    
    val request = ForecastQuery(params)
    val response = ask(actor,request)     

    response.onSuccess {
        
      case result => {

        if (result.isInstanceOf[InsightForecasts]) {
          ctx.complete(result.asInstanceOf[InsightForecasts])
            
        } else if (result.isInstanceOf[SimpleResponse]) {
          ctx.complete(result.asInstanceOf[SimpleResponse])
            
        } else {
          /* do nothing */
        }
          
      }
      
    }

    response.onFailure {
      case throwable => ctx.complete(throwable.getMessage)
    }      
  
  }
  
  private def doLoyalty[T](ctx:RequestContext) = {

    implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
    val actor = system.actorOf(Props(new query.LoyaltyQuestor(requestCtx)))
    val params = getRequest(ctx)
    
    val request = LoyaltyQuery(params)
    val response = ask(actor,request)     

    response.onSuccess {
        
      case result => {

        if (result.isInstanceOf[InsightLoyalties]) {
          ctx.complete(result.asInstanceOf[InsightLoyalties])
            
        } else if (result.isInstanceOf[SimpleResponse]) {
          ctx.complete(result.asInstanceOf[SimpleResponse])
            
        } else {
          /* do nothing */
        }
          
      }
      
    }

    response.onFailure {
      case throwable => ctx.complete(throwable.getMessage)
    }      
  
  }
  
  private def doRecommendation[T](ctx:RequestContext) = {

    implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
    val actor = system.actorOf(Props(new query.RecommendationQuestor(requestCtx)))
    val params = getRequest(ctx)
    
    val request = RecommendationQuery(params)
    val response = ask(actor,request)     

    response.onSuccess {
        
      case result => {

        if (result.isInstanceOf[InsightRecommendations]) {
          ctx.complete(result.asInstanceOf[InsightRecommendations])
            
        } else if (result.isInstanceOf[SimpleResponse]) {
          ctx.complete(result.asInstanceOf[SimpleResponse])
            
        } else {
          /* do nothing */
        }
          
      }
      
    }

    response.onFailure {
      case throwable => ctx.complete(throwable.getMessage)
    }      
  
  }

  /**
   * A 'task' request supports the retrieval of the registered
   * preparation or synchronization tasks
   */
  private def doTask[T](ctx:RequestContext) = {

    implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
    val actor = system.actorOf(Props(new query.TaskQuestor(requestCtx)))
    val params = getRequest(ctx)
    
    val request = TaskQuery(params)
    val response = ask(actor,request)     

    response.onSuccess {
        
      case result => {

        if (result.isInstanceOf[InsightTasks]) {
          ctx.complete(result.asInstanceOf[InsightTasks])
            
        } else if (result.isInstanceOf[SimpleResponse]) {
          ctx.complete(result.asInstanceOf[SimpleResponse])
            
        } else {
          /* do nothing */
        }
          
      }
      
    }

    response.onFailure {
      case throwable => ctx.complete(throwable.getMessage)
    }      
  
  }
  
  /**
   * 'product' supports retrieval of product related mining and prediction
   * results; these requests are completely independent of a certain user 
   * and focus on relations between different products.
   */
  private def doProduct[T](ctx:RequestContext,method:String) = {
    
    if (List(
        /* association rule related */
        "product_cross_sell",
        "product_promotion",
        "product_suggest",
        /* stats related */
        "product_top_sell").contains(method)) {
      
      implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
      val actor = system.actorOf(Props(new query.ProductQuestor(requestCtx)))
      val params = getRequest(ctx)
      
      val excludes = List(Names.REQ_METHOD)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_METHOD -> method)
    
      val request = ProductQuery(data)
      val response = ask(actor,request)     

      response.onSuccess {
        
        case result => {

          if (result.isInstanceOf[InsightFilteredItems]) {
            /*
             * product_cross_sell
             * product_promotion
             * product_suggest
             */
            ctx.complete(result.asInstanceOf[InsightFilteredItems])
            
          } else if (result.isInstanceOf[InsightTopItems]) {
            /* product_top_sell */
            ctx.complete(result.asInstanceOf[InsightTopItems])
            
          } else if (result.isInstanceOf[SimpleResponse]) {
            ctx.complete(result.asInstanceOf[SimpleResponse])
            
          } else {
            /* do nothing */
          }
          
        }
      
      }

      response.onFailure {
        case throwable => ctx.complete(throwable.getMessage)
      }
      
    }
      
  }
  
  private def doUser[T](ctx:RequestContext,method:String) = {
    
    if (List(
        "user_forecast",
        "user_loyalty",
        "user_recommendation").contains(method)) {
      
      implicit val timeout:Timeout = DurationInt(requestCtx.getTimeout).second      
 
      val actor = system.actorOf(Props(new query.UserQuestor(requestCtx)))
      val params = getRequest(ctx)
      
      val excludes = List(Names.REQ_METHOD)
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_METHOD -> method)
    
      val request = UserQuery(data)
      val response = ask(actor,request)     

      response.onSuccess {
        
        case result => {

          if (result.isInstanceOf[InsightForecasts]) {
            /* user_forecast */
            ctx.complete(result.asInstanceOf[InsightForecasts])

          } else if (result.isInstanceOf[InsightLoyalties]) {
            /* user_loyalty */
            ctx.complete(result.asInstanceOf[InsightLoyalties])
 
          } else if (result.isInstanceOf[InsightFilteredItems]) {
            /* user_recommendation */
            ctx.complete(result.asInstanceOf[InsightFilteredItems])
            
          } else if (result.isInstanceOf[SimpleResponse]) {
            ctx.complete(result.asInstanceOf[SimpleResponse])
            
          } else {
            /* do nothing */
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