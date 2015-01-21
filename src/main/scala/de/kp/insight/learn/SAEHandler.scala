package de.kp.insight.learn
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.shopify.insight.model._

class SAEHandler(ctx:RequestContext) {

  def train(params:Map[String,String]):Map[String,String] = { 
    
    val base = ctx.getBase
    val name = params(Names.REQ_NAME)
    
    val uid = params(Names.REQ_UID)
    val url = String.format("""%s/%s/%s/1""", base, name, uid)
 
    val k = params.get("k") match {
      case None => 8.toString
      case Some(value) => value
    }
     
    val iterations = params.get("iterations") match {
      case None => 20.toString
      case Some(value) => value
    }
   
    params ++ Map(
      /* Algorithm specification */
      Names.REQ_ALGORITHM -> "KMEANS",
      
      "k" -> k,
      "iterations" -> iterations,
      
      /* Data source description */
      Names.REQ_URL -> url,        
      Names.REQ_SOURCE -> "PARQUET"
    )
  }

}