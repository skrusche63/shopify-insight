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

import java.util.Date

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.routing.RoundRobinRouter

import de.kp.shopify.insight.{Configuration,RemoteContext}
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.cache.ActorMonitor

import scala.concurrent.duration.DurationInt

class MonitoredActor(name:String) extends Actor with ActorLogging {

  val (heartbeat,time) = Configuration.actor      
  val (duration,retries,workers) = Configuration.router  
  
  implicit val ec = context.dispatcher
  val scheduledTask = context.system.scheduler.schedule(DurationInt(0).second, DurationInt(1).second,self,new AliveMessage())  
 
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(duration).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  protected val ctx = new RemoteContext()
  protected val router = context.actorOf(Props(new WorkerActor(ctx)).withRouter(RoundRobinRouter(workers)))
  
  override def postStop() {
    scheduledTask.cancel()
  }  

  def receive = {
    /*
     * Message sent by the scheduler to track the 'heartbeat' of this actor
     */
    case req:AliveMessage => register(name)
    /*
     * Message sent to interact with a remote actor specifying the access
     * point of a certain prediction engine, e.g. association, context etc
     */
    case req:ServiceRequest => {
      
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
      val response = ask(router, req)
      
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! failure(req)      
	  }
      
    }
    case _ => {}
    
  }
   
  def failure(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")    
    new ServiceResponse(req.service,req.task,Map("uid" -> uid),ResponseStatus.FAILURE)	
  
  }
 
  protected def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map("message" -> message)
      new ServiceResponse("","",data,ResponseStatus.FAILURE)	
      
    } else {
      val data = Map("uid" -> req.data("uid"), "message" -> message)
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
    
    }
    
  }

  def register(name:String) {
      
    val now = new Date()
    val ts = now.getTime()

    ActorMonitor.add(ActorInfo(name,ts))

  }

}