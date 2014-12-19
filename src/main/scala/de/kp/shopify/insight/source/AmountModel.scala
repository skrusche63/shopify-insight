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

import scala.collection.mutable.ArrayBuffer

private case class Pair(time:Long,state:String)

class AmountModel(@transient sc:SparkContext) extends Serializable {
 
  private val handler = AmountHandler
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[(String,String,Long,String)] = {
    
    val purchases = rawset.map(data => {
      
      val site = data(Names.SITE_FIELD)
      val user = data(Names.USER_FIELD)      

      val timestamp = data(Names.TIMESTAMP_FIELD).toLong
      val amount  = data(Names.AMOUNT_FIELD).toFloat
      
      (site,user,timestamp,amount)
      
    })
   
    buildStates(purchases)
    
  }

  /**
   * Represent transactions as a time ordered sequence of Markov States;
   * the result is directly used to build the respective Markov Model
   */
  def buildStates(sequences:RDD[(String,String,Long,Float)]):RDD[(String,String,Long,String)] = {
    
    /*
     * Group purchases by site & user and restrict to those
     * users with more than one purchase
     */
    sequences.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).flatMap(p => {

      val (site,user) = p._1
      val orders = p._2.map(v => (v._3,v._4)).toList.sortBy(_._1)
      
      /* Extract first order */
      var (pre_time,pre_amount) = orders.head
      val states = ArrayBuffer.empty[Pair]

      for ((time,amount) <- orders.tail) {
        
        /* Determine state from amount */
        val astate = handler.stateByAmount(amount,pre_amount)
     
        /* Determine state from time elapsed between
         * subsequent orders or transactions
         */
        val tstate = handler.stateByTime(time,pre_time)
      
        val state = astate + tstate
        states += Pair(time,state)
        
        pre_amount = amount
        pre_time   = time
        
      }
      
      states.map(p => (site,user,p.time,p.state))
      
    })
    
  }
}