package de.kp.shopify.insight.actor
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

import org.apache.spark.SparkContext
import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.Configuration

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.HashMap

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class Finder(name:String,listener:ActorRef) extends MasterActor(name) {

  override def execute(req:ServiceRequest):Future[Any] = {
    
    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second

    val actor = context.actorOf(Props(new RemoteQuestor(ctx,listener)))
    ask(actor,req)
    
  }
  
}

class Monitor(name:String) extends MasterActor(name) {
  
  override def execute(req:ServiceRequest):Future[Any] = {
    
    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second

    val actor = context.actorOf(Props(new StatusQuestor(Configuration)))
    ask(actor,req)
    
  }
  
}