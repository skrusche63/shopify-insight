package de.kp.shopify.insight.storage
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

object CCNLoaderApp extends LoaderApp("CCNLoader") {
  
  def main(args:Array[String]) {

    try {

      val params = createParams(args)
      val name = "CCN"
 
      val req_params = params ++ Map("name" -> name)
      initialize(req_params)

      val actor = system.actorOf(Props(new CCNHandler(ctx)))   
      inbox.watch(actor)
    
      actor ! StartLoad(req_params)

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

  class CCNHandler(ctx:RequestContext) extends Actor {
    
    override def receive = {
    
      case msg:StartLoad => {

        val start = new java.util.Date().getTime     
        println("CCNLoaderApp started at " + start)
        
        val actor = context.actorOf(Props(new CCNLoader(ctx)))          
        actor ! StartLoad(msg.data)
       
      }
    
      case msg:LoadFailed => {
    
        val end = new java.util.Date().getTime           
        println("CCNLoaderApp failed at " + end)
    
        context.stop(self)
      
      }
    
      case msg:LoadFinished => {
    
        val end = new java.util.Date().getTime           
        println("CCNLoaderApp finished at " + end)
    
        context.stop(self)
    
      }
    
    }
  
  }
  
}