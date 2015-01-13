package de.kp.shopify.insight.prepare
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

import org.apache.spark.rdd.RDD
import akka.actor._

import de.kp.shopify.insight.RequestContext
import de.kp.shopify.insight.model._

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.HashMap

object RFMPreparerApp extends PreparerApp("RFMPreparer") {
  
  def main(args:Array[String]) {

    try {

      /*
       * Add internal arguments to request parameters; one of
       * these arguments is the name of the respective task
       */
      val req_params = createParams(args) ++ Map("name" -> "RFM")
      /*
       * Load orders from Elasticsearch order database and 
       * start Preparer actor to extract RFM data from the
       * different purchase transactions and store the result 
       * as a Parquet file
       */
      val orders = initialize(req_params)
      /*
       * Start & monitor PreparerActor
       */
      val actor = system.actorOf(Props(new RFMHandler(ctx,orders)))   
      inbox.watch(actor)
    
      actor ! StartPrepare(req_params)

      val timeout = DurationInt(30).minute
    
      while (inbox.receive(timeout).isInstanceOf[Terminated] == false) {}    
      sys.exit
      
    } catch {
      case e:Exception => {
          
        println(e.getMessage) 
        sys.exit
          
      }
    
    }

  }

  class RFMHandler(ctx:RequestContext,orders:RDD[InsightOrder]) extends Actor {
    
    override def receive = {
    
      case msg:StartPrepare => {

        val start = new java.util.Date().getTime     
        println("RFMPreparerApp started at " + start)
 
        /*
         * Preparation of RFM perspective is independent
         * of the customer type; we therefore set customer
         * to '0' to satisfy the interface
         */
        val customer = 0
        
        val preparer = context.actorOf(Props(new RFMPreparer(ctx,customer,orders)))          
        preparer ! StartPrepare(msg.data)
       
      }
    
      case msg:PrepareFailed => {
    
        val end = new java.util.Date().getTime           
        println("RFMPreparerApp failed at " + end)
    
        context.stop(self)
      
      }
    
      case msg:PrepareFinished => {
    
        val end = new java.util.Date().getTime           
        println("RFMPreparerApp finished at " + end)
    
        context.stop(self)
    
      }
    
    }
  
  }
  
}