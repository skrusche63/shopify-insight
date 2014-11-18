package de.kp.shopify.insight.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ActorInfo(
  name:String,timestamp:Long
)

case class AliveMessage()

case class ActorStatus(
  name:String,date:String,status:String
)

case class ActorsStatus(items:List[ActorStatus])

object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}

object Services {
  /*
   * Shopifyinsight. supports Association Analysis; the respective request
   * is delegated to Predictiveworks.
   */
  val ASSOCIATION:String = "association"
  /*
   * Shopifyinsight. supports Intent Recognition; the respective request is
   * delegated to Predictiveworks.
   */
  val INTENT:String = "intent"
  /*
   * Recommendation is an internal service of PIWIKinsight and uses ALS to
   * predict the most preferred item for a certain user
   */
  val RECOMMENDATION:String = "recommendation"
  /*
   * Shopifyinsight. supports Series Analysis; the respectiv request is 
   * delegated to Predictiveworks.
   */
  val SERIES:String = "series"
    
  private val services = List(ASSOCIATION,INTENT,SERIES)
  
  def isService(service:String):Boolean = services.contains(service)
  
}
/*
 * Request-Response protocol to interaction with Predictiveworks.
 */
case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)
