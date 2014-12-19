package de.kp.shopify.insight.source
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
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.source._

import de.kp.shopify.insight.Configuration
import de.kp.shopify.insight.model._
/**
 * AmountSource is part of the preparation or preprocessing
 * functionality and supports the transformation of a certain
 * 'amount' database into a 'state' database
 */
class AmountSource(@transient sc:SparkContext) extends Serializable {

  private val config = Configuration
  private val model = new AmountModel(sc)
  /**
   * This method retrieves data 'amount' based data from an Elasticsearch index
   * and transforms them into a state representation that can be used to train
   * either a Markov model or a Hidden Markov (Intent Recognition)
   */
  def get(req:ServiceRequest):RDD[(String,String,Long,String)] = {

    val uid = req.data(Names.REQ_UID)

    val source = req.data(Names.REQ_SOURCE)
    source match {

      case Sources.ELASTIC => {        

        val rawset = new ElasticSource(sc).connect(config,req)
        model.buildElastic(req,rawset)        
      
      }
            
      case _ => null
      
    }
    
  }

}