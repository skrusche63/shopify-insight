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

import akka.actor.Props

import de.kp.spark.core.Names

import de.kp.shopify.insight.PrepareContext
import de.kp.shopify.insight.model._

class SyncPipeline(prepareContext:PrepareContext) extends BaseActor {

  override def receive = {
    
    case message:StartPipeline => {
      
      /**********************************************************************
       *      
       *                       SUB PROCESS 'SYNCHRONIZE'
       * 
       *********************************************************************/
      try {    
        
    
      } catch {
        
        case e:Exception => {
          /*
           * Inform the message listener about the error that occurred
           * while collecting data from a certain Shopify store and
           * stop the DataPipeline
           */
          prepareContext.listener ! e.getMessage
          
          prepareContext.clear
          context.stop(self)
          
        }

      } 
      
    }
  
  }
}