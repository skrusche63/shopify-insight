package de.kp.shopify.insight.cache
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

import de.kp.shopify.insight.Configuration
import de.kp.shopify.insight.model._

import scala.collection.mutable.ArrayBuffer

object ActorMonitor {
  
  private val (heartbeat,time) = Configuration.actor
  
  private val size = Configuration.cache
  private val cache = new LRUCache[String,ArrayBuffer[Long]](size)

  def add(info:ActorInfo) {
    
    val k = info.name
    cache.get(k) match {
      case None => {
        
        val buffer = ArrayBuffer.empty[Long]
        buffer += info.timestamp
      
        cache.put(k,buffer)
        
      }
      
      case Some(buffer) => buffer += info.timestamp
      
    }
    
    
  }
  
  def isAlive(names:Seq[String]):ActorsStatus = {
    
    val now = new Date().toString()
    val actors = ArrayBuffer.empty[ActorStatus]
    
    for (name <- names) {
      
      val status = if (isAlive(name)) "active" else "inactive"
      actors += ActorStatus(name,now,status)
      
    }
    
    ActorsStatus(actors.toList)
  
  }
  
  def isAlive(name:String):Boolean = {
    
    val alive = cache.get(name) match {
      
      case None => false
      
      case Some(buffer) => {
        
        val last = buffer.last
        val now = new Date()
        
        val ts = now.getTime()
        if (ts - last > heartbeat * 1000) false else true
        
      }
      
    }

    alive
    
  }

}