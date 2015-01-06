package de.kp.shopify.insight.analytics
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

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import org.joda.time.DateTime
import scala.collection.JavaConversions._

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * The 'Analytics' class evaluates purchase transactions of a certain
 * time span, e.g. 30, 60 or 90 days and different data dimensions.
 * 
 * This kind of evaluation is done within the preparation phase for
 * subsequent data mining and model building, and concentrates on
 * the item, monetary and temporal dimension
 */
class Analytics(requestCtx:RequestContext) {
  /*
   * These customer lifetime value thresholds refer to 
   * the sigma-range of the average lifetime value and
   * are leveraged to classify customers as "high", "norm"
   * and "low" customers
   */
  private val CLV_MAX_THRESHOLD = 1.3
  private val CLV_MIN_THRESHOLD = 0.7
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  def buildSUM(params:Map[String,String],rawset:List[Order]):XContentBuilder = {
    
    val uid = params(Names.REQ_UID)
    val total = rawset.size
    /*
     * Extract the temporal and monetary dimension from the raw dataset
     */
    val orders_rfm = rawset.map(order => (order.site,order.user,order.timestamp,order.amount))    
    
    /********************* MONETARY DIMENSION ********************/
    
    /*
     * Compute the average, minimum and maximum amount of all 
     * the purchase transactions of last 30, 60 or 90 days
     */    
    val amounts = orders_rfm.map(_._4)
    val total_amount = amounts.foldLeft(0.toFloat)(_ + _)
    
    val total_avg_amount = total_amount / total.toFloat
    
    val total_min_amount = amounts.min
    val total_max_amount = amounts.max
    /*
     * We caluculate the average, minimum and maximum customer value 
     * from the amount a customer has spent in the certain timespan 
     * under consideration
     */
    val (total_avg_clv,total_min_clv,total_max_clv) = statsCLV(orders_rfm)
    
    /********************* TEMPORAL DIMENSION ********************/
    
    /*
     * Compute the average,minimum and maximum time elapsed between 
     * two subsequent transactions; the 'zip' method is used to pairs 
     * between two subsequent timestamps
     */
    val timestamps = orders_rfm.map(_._3).sorted
    val timespans = timestamps.zip(timestamps.tail).map(x => x._2 - x._1)
    
    val total_avg_timespan = timespans.foldLeft(0.toLong)(_ + _) / total.toLong

    val total_min_timespan = timespans.min
    val total_max_timespan = timespans.max

    /*
     * Besides the evaluation of the time elasped between subsequent
     * transactions, we also compute the preferred days of the orders,
     * and the preferred time of the day. The preference is computed as 
     * follows:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     * 
     */

    /* Day of the week: 1..7 */
    val total_day_supp = timestamps.map(x => {

        val date = new java.util.Date()
        date.setTime(x)
      
        val datetime = new DateTime(date)
        val dow = datetime.dayOfWeek().get
        
        dow
       
    }).groupBy(x => x).map(x => (x._1,x._2.size))

    val total_day_pref = total_day_supp.map(x => {
       
       val (day,supp) = x 
       val pref = Math.log(1 + supp.toDouble / total.toDouble)    
       
       (day,pref)
       
     })

    /* Time of the day: 1..4 */
    val total_time_supp = timestamps.map(x => {

        val date = new java.util.Date()
        date.setTime(x)
      
        val datetime = new DateTime(date)
        val hod = datetime.hourOfDay().get
        
        hod
       
    }).groupBy(x => x).map(x => (x._1,x._2.size))
 
    val total_time_pref = total_time_supp.map(x => {
       
       val (time,supp) = x 
       val pref = Math.log(1 + supp.toDouble / total.toDouble)    
       
       (time,pref)
       
     })
    
    /*********************** ITEM DIMENSION **********************/

     /*
     * Extract the item dimension from the raw dataset
     */
    val orders_itm = rawset.map(order => {
      
      val items = order.items.map(x => (x.item,x.quantity))
      (order.site,order.user,order.group,order.timestamp,items)
      
    })

    /*
     * Compute the preferred items of the orders, which is:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     * 
     */
    val total_item_supp = orders_itm.flatMap(_._5).groupBy(x => x._1).map(x => {
      
      val item = x._1
      val supp = x._2.map(_._2).sum
      
      (item,supp)
    })

    val total_item_pref = total_item_supp.map(x => {
       
       val (item,supp) = x 
       val pref = Math.log(1 + supp.toDouble / total.toDouble)    
       
       (item,pref)
       
    })
          
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	    
	/* uid */
	builder.field("uid",uid)
	    
	/* timestamp */
	builder.field("timestamp",new java.util.Date().getTime)

	/* created_at_min */
	builder.field("created_at_min",params("created_at_min"))

	/* created_at_max */
	builder.field("created_at_max",params("created_at_max"))
	    
	/*
	 * Denormalized description of the RFM data retrieved
	 * from all transactions taken into account, i.e. from 
	 * the 30, 60 or 90 days
	 */
	    
    /* total_orders */
	builder.field("total_orders",total)

	/* total_amount */
	builder.field("total_amount",total_amount)
	
	/* total_avg_amount */
	builder.field("total_avg_amount",total_avg_amount)

	/* total_max_amount */
	builder.field("total_max_amount",total_max_amount)

	/* total_min_amount */
	builder.field("total_min_amount",total_min_amount)

	/* total_avg_clv */
	builder.field("total_avg_clv",total_avg_clv)

	/* total_max_clv */
	builder.field("total_max_clv",total_max_clv)

	/* total_min_clv */
	builder.field("total_min_clv",total_min_clv)
 
	/* total_avg_timespan */
	builder.field("total_avg_timespan",total_avg_timespan)

	/* total_max_timespan */
	builder.field("total_max_timespan",total_max_timespan)

	/* total_min_timespan */
	builder.field("total_min_timespan",total_min_timespan)

	/* total_day_supp */
	builder.startArray("total_day_supp")
	for (rec <- total_day_supp) {
      builder.startObject()
      /*
       * Note, that NOT ALL days of the week have to
       * be present here
       */
      builder.field("day", rec._1)
      builder.field("supp",rec._2)
              
      builder.endObject()
    
	}

    builder.endArray()

	/* total_day_pref */
	builder.startArray("total_day_pref")
	for (rec <- total_day_pref) {
      builder.startObject()
      /*
       * Note, that NOT ALL days of the week have to
       * be present here
       */
      builder.field("day",  rec._1)
      builder.field("score",rec._2)
              
      builder.endObject()
    
	}

    builder.endArray()

    /* total_time_supp */
	builder.startArray("total_time_supp")
	for (rec <- total_time_supp) {
      builder.startObject()
      /*
       * Note, that NOT ALL time peridos of the day
       * have to be present here
       */
              
      builder.field("time",rec._1)
      builder.field("supp",rec._2)
              
      builder.endObject()
    
	}

    builder.endArray()

    /* total_time_pref */
	builder.startArray("total_time_pref")
	for (rec <- total_time_pref) {
      builder.startObject()
      /*
       * Note, that NOT ALL time peridos of the day
       * have to be present here
       */
              
      builder.field("time", rec._1)
      builder.field("score",rec._2)
              
      builder.endObject()
    
	}

    builder.endArray()
   

	/* total_item_supp */
	builder.startArray("total_item_supp")
	for (rec <- total_item_supp) {

	  builder.startObject()
          
	  builder.field("item",rec._1)
      builder.field("supp",rec._2)
              
      builder.endObject()
    }

    builder.endArray()
    
	/* total_item_pref */
	builder.startArray("total_item_pref")
	for (rec <- total_item_pref) {

	  builder.startObject()
          
	  builder.field("item", rec._1)
      builder.field("score",rec._2)
              
      builder.endObject()
    }
    
    builder.endArray()
    
    builder.endObject()
    builder
    
  }
  
  private def statsCLV(rawset:List[(String,String,Long,Float)]):(Float,Float,Float) = {
    
    val values = rawset.groupBy(x => (x._1,x._2)).map(x => x._2.map(_._4).sum)
    val total_avg_clv = values.sum / values.size
    
    val total_min_clv = values.min
    val total_max_clv = values.max
    
    (total_avg_clv,total_min_clv,total_max_clv)
    
  }
  
  private def timeOfDay(hour:Int):Int = {

    val timeOfDay = if (0 < hour && hour < 7) {
      /*
       * The period from midnight, 0, to 6
       * specifies the period before work
       */
      1
    
    } else if (7 <= hour && hour < 13) {
      /*
       * The period from 7 to 12 specifies
       * the morning period of a day
       */
      2
    
    } else if (13 <= hour && hour < 18) {
      /*
       * The period from 13 to 17 specifies
       * the afternoon period of a day
       */
      3
    
    } else {
      /*
       * The period from 18 to 23 specifies
       * the evening periof of a day
       */
      4
    }

    timeOfDay
    
  }
  
}