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

import org.clapper.argot._

import de.kp.shopify.insight.RequestContext
import de.kp.shopify.insight.model._

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.HashMap

object LOCPreparerApp extends PreparerApp {
  
  private val PROGRAM_NAME = "Geo Preparer"
  
  def main(args:Array[String]) {

    import ArgotConverters._
    try {

      val parser = new ArgotParser(
        programName = PROGRAM_NAME,
        compactUsage = true,
        preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
      )
    
      /*
       * The 'uid' parameter must be provided with the -uid option
       */
      val uid = parser.option[String](List("uid"),"uid","Unique preparation identifier")
      /*
       * The subsequent parameters specify a certain period of time with 
       * a minimum and maximum date
       */
      val created_at_min = parser.option[String](List("min_date"),"created_at_min","Data created after this date.")
      val created_at_max = parser.option[String](List("max_date"),"created_at_max","Data created before this date.")
    
      parser.parse(args)
    
      val params = HashMap.empty[String,String]
      
      if (uid.hasValue == false)
        throw new Exception("Parameter 'uid' is missing.")
      
      if (created_at_min.hasValue == false)
        throw new Exception("Parameter 'min_date' is missing.")
      
      if (created_at_max.hasValue == false)
        throw new Exception("Parameter 'max_date' is missing.")
      
      /*
       * Add external arguments to request parameters
       */
      params += "uid" -> uid.value.get
      
      params += "created_at_min" -> created_at_min.value.get
      params += "created_at_max" -> created_at_max.value.get

      val req_params = params.toMap
      /*
       * Load orders from Elasticsearch order database and 
       * start Preparer actor to extract geospatial data
       * from the different purchase transactions and store
       * the result as a Parquet file
       */
      val orders = initialize(req_params)
      /*
       * Start & monitor PreparerActor
       */
      val actor = system.actorOf(Props(new PreparerActor(ctx,orders)))   
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
  
}

class PreparerActor(ctx:RequestContext,orders:RDD[InsightOrder]) extends Actor {
    
  override def receive = {
    
    case msg:StartPrepare => {

      val start = new java.util.Date().getTime     
      println("LOCPreparerApp started at " + start)
      
    }
    
    case msg:PrepareFailed => {
    
      val end = new java.util.Date().getTime           
      println("LOCPreparerApp failed at " + end)
    
      context.stop(self)
      
    }
    
    case msg:PrepareFinished => {
    
      val end = new java.util.Date().getTime           
      println("LOCPreparerApp finished at " + end)
    
      context.stop(self)
    
    }
    
  }
}