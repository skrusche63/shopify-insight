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

  def buildITM(params:Map[String,String],rawset:List[Order]):List[XContentBuilder] = {
    
    val uid = params(Names.REQ_UID)
    val total = rawset.size
    
    /*
     * Extract the item dimension from the raw dataset
     */
    val orders = rawset.map(order => {
      
      val items = order.items.map(x => (x.item,x.quantity))
      (order.site,order.user,order.group,order.timestamp,items)
      
    })
    /*
     * Compute the preferred items of the orders, which is:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     * 
     */
    val total_item_supp = orders.flatMap(_._5).groupBy(x => x._1).map(x => {
      
      val item = x._1
      val supp = x._2.map(_._2).foldLeft(0.toInt)(_ + _)
      
      (item,supp)
    })

    val total_item_pref = total_item_supp.map(x => {
       
       val (item,supp) = x 
       val pref = Math.log(1 + supp.toDouble / total.toDouble)    
       
       (item,pref)
       
    })
     
    orders.groupBy(x => (x._1,x._2)).flatMap(p => {
       
      val (site,user) = p._1
       
      /* Compute time ordered list of (group,timestamp,items) */
      val user_orders = p._2.map(x => (x._3,x._4,x._5)).toList.sortBy(_._1)      
      val user_total = user_orders.size
       
      /*
       * Compute the preferred items of the orders, which is:
       * 
       * pref = Math.log(1 + supp.toDouble / total.toDouble)
       * 
       */
      val user_item_supp = user_orders.flatMap(_._3).groupBy(x => x._1).map(x => {
      
        val item = x._1
        val supp = x._2.map(_._2).foldLeft(0.toInt)(_ + _)
      
        (item,supp)
    
      })

      val user_item_pref = user_item_supp.map(x => {
       
        val (item,supp) = x 
        val pref = Math.log(1 + supp.toDouble / total.toDouble)    
        /*
         * Normalize the user preference with respect to the
         * total item preference
         */
        val gpref = total_item_pref(item)
        val npref = pref.toDouble / gpref.toDouble
       
        (item,npref)
       
      })
    
      user_orders.flatMap(x => {
        
        val (group,timestamp,items) = x
        items.map(x => {
        
          val (item,quantity) = x
          /*
           * Determine product that refers to the 'item' identifier
           * from the synchronized (hopefully before) product database
           * and retrieve 'tags' and 'category'
           */
          val product = requestCtx.get("database","products",item.toString)
          
          val tags = product("tags").asInstanceOf[String]
          val category = product("category").asInstanceOf[String]

          val score = user_item_pref(item)
          
          val builder = XContentFactory.jsonBuilder()
	      builder.startObject()
        
	      /*
	       * The subsequent fields are shared with Predictiveworks'
	       * Intent Recognition engine and must also be described 
	       * by a field or metadata specification
	       */
	      
	      /* uid */
	      builder.field(Names.UID_FIELD,uid)
	      
	      /* site */
	      builder.field(Names.SITE_FIELD,site)
	    
	      /* user */
	      builder.field(Names.USER_FIELD,user)
	    
	      /* timestamp */
	      builder.field(Names.TIMESTAMP_FIELD,timestamp)
	    
	      /* group */
	      builder.field(Names.GROUP_FIELD,group)
	    
	      /* item */
	      builder.field(Names.ITEM_FIELD,item)
	      
	      /* score */
	      builder.field(Names.SCORE_FIELD,score)

	      /*
	       * The subsequent fields are used for evaluation within
	       * the Shopify insight server
	       */

	      /* created_at_min */
	      builder.field("created_at_min",params("created_at_min"))

	      /* created_at_max */
	      builder.field("created_at_max",params("created_at_max"))
	    
	      /* user_total */
	      builder.field("user_total",user_total)

	      /* item_quantity */
	      builder.field("item_quantity",quantity)
	    
	      /* item_category */
	      builder.field("item_category",category)
	    
	      /* item_tags */
	      builder.field("item_tags",tags)

	      builder.endObject()
	    
	      builder
        
        })
      })
    
    }).toList 

  }

  /**
   * This method computes the monetary and temporal dimension from
   * purchase transactions; processing goes alongside with the RFM
   * model
   */
  def buildRFM(params:Map[String,String],rawset:List[Order]):List[XContentBuilder] = {
    
    val uid = params(Names.REQ_UID)
    val total = rawset.size

    /*
     * Extract the temporal and monetary dimension from the raw dataset
     */
    val orders = rawset.map(order => (order.site,order.user,order.timestamp,order.amount))    
    
    /*
     * We compute the preferred days of the orders, and the preferred 
     * time of the day. The preference is computed as follows:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     * 
     */
    val timestamps = orders.map(_._3).sorted

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
     
    /*
     * Group orders by site & user and restrict to those
     * users with more than one purchase
     */
    orders.groupBy(x => (x._1,x._2)).filter(_._2.size > 1).flatMap(p => {

      val (site,user) = p._1  
      
      /* Compute time ordered list of (timestamp,amount) */
      val user_orders = p._2.map(x => (x._3,x._4)).toList.sortBy(_._1)      
      val user_total = user_orders.size
      /*
       * For each user, we calculate an RFM model, where RFM stands for
       * 
       * a) recency: how recently did the customer purchase
       * 
       * b) frequency: how often did the customer purchase
       * 
       * c) monetary value: how much did the customer spent
       */
     
      /******************** MONETARY DIMENSION *******************/
     
      /*
       * Compute the average, minimum and maximum amount of all 
       * the user purchase transactions of last 30, 60 or 90 days
       */    
      val user_amounts = user_orders.map(_._2)
 
      val user_total_spent = user_amounts.foldLeft(0.toFloat)(_ + _)      
      val user_avg_amount = user_total_spent / user_total.toFloat
    
      val user_min_amount = user_amounts.min
      /*
       * (RF)M: the value of the highest order from the given customer is
       * used for the M part of the RFM model
       */
      val user_max_amount = user_amounts.max
      
      /*
       * Compute the amount sub states from the subsequent pairs 
       * of user amounts; the amount handler holds a predefined
       * configuration to map a pair onto a state
       */
      val user_amount_states = user_amounts.zip(user_amounts.tail).map(x => StateHandler.stateByAmount(x._2,x._1))
     
      /******************** TEMPORAL DIMENSION *******************/

      /*
       * Compute the average,minimum and maximum time elapsed between 
       * two subsequent user transactions; the 'zip' method is used to 
       * pairs between two subsequent timestamps
       */
      val user_timestamps = user_orders.map(_._1)
      val user_timespans = user_timestamps.zip(user_timestamps.tail).map(x => x._2 - x._1)
    
      val user_avg_timespan = user_timespans.foldLeft(0.toLong)(_ + _) / user_total.toLong

      val user_min_timespan = user_timespans.min
      val user_max_timespan = user_timespans.max
      /*
       * R(FM): the time elapsed since the last transaction and
       * today; this data value is used to specify 'recency'
       */
      val today = new java.util.Date().getTime()
      val user_recency = user_timestamps.last - today
    
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
      val user_day_supp = user_timestamps.map(x => {

        val date = new java.util.Date()
        date.setTime(x)
      
        val datetime = new DateTime(date)
        val dow = datetime.dayOfWeek().get
        
        dow
        
      }).groupBy(x => x).map(x => (x._1,x._2.size))

      val user_day_pref = user_day_supp.map(x => {
       
        val (day,supp) = x 
        val pref = Math.log(1 + supp.toDouble / total.toDouble)    

        /*
         * Normalize the user preference with respect to the
         * total day preference
         */
        val gpref = total_day_pref(day)
        val npref = pref.toDouble / gpref.toDouble

        (day,npref)
       
      })

      /* Time of the day: 1..4 */
      val user_time_supp = user_timestamps.map(x => {

        val date = new java.util.Date()
        date.setTime(x)

        val datetime = new DateTime(date)
        val hod = datetime.hourOfDay().get
        
        hod
        
      }).groupBy(x => x).map(x => (x._1,x._2.size))
     
      val user_time_pref = user_time_supp.map(x => {
       
        val (time,supp) = x 
        val pref = Math.log(1 + supp.toDouble / total.toDouble)    
        /*
         * Normalize the user preference with respect to the
         * total time preference
         */
        val gpref = total_time_pref(time)
        val npref = pref.toDouble / gpref.toDouble
       
        (time,npref)
       
      })
       
      /*
       * Compute the timespan sub states from the subsequent pairs 
       * of user timestamps; the amount handler holds a predefined
       * configuration to map a pair onto a state
       */
      val user_time_states = user_timestamps.zip(user_timestamps.tail).map(x => StateHandler.stateByTime(x._2,x._1))

      /*
       * Build user states and zip result with user_timestamps
       */
      val user_zipped_states = user_amount_states.zip(user_time_states).map(x => x._1 + x._2).zip(user_timestamps.tail)
      
      user_zipped_states.map(x => {
          
        val builder = XContentFactory.jsonBuilder()
	    builder.startObject()
        
	    /*
	     * The subsequent fields are shared with Predictiveworks'
	     * Intent Recognition engine and must also be described 
	     * by a field or metadata specification
	     */
	    /* site */
	    builder.field(Names.SITE_FIELD,site)
	    
	    /* user */
	    builder.field(Names.USER_FIELD,user)

	    /* timestamp */
	    builder.field(Names.TIMESTAMP_FIELD,x._2)
	    
	    /* state */
	    builder.field(Names.STATE_FIELD,x._1)

	    /*
	     * The subsequent fields are used for evaluation within
	     * the Shopify insight server
	     */
	    
	    /* uid */
	    builder.field(Names.UID_FIELD,uid)

	    /* created_at_min */
	    builder.field("created_at_min",params("created_at_min"))

	    /* created_at_max */
	    builder.field("created_at_max",params("created_at_max"))
	    
	    /*
	     * Denormalized description of the RFM data retrieved
	     * from all user specific transactions taken into account, 
	     * i.e. from the 30, 60 or 90 days
	     */
	    
	    /* user_total */
	    builder.field("user_total",user_total)

	    /* user_total_spent */
	    builder.field("user_total_spent",user_total_spent)

	    /* user_avg_amount */
	    builder.field("user_avg_amount",user_avg_amount)

	    /* user_max_amount */
	    builder.field("user_max_amount",user_max_amount)

	    /* user_min_amount */
	    builder.field("user_min_amount",user_min_amount)
 
	    /* user_avg_timespan */
	    builder.field("user_avg_timespan",user_avg_timespan)

	    /* user_max_timespan */
	    builder.field("user_max_timespan",user_max_timespan)

	    /* user_min_timespan */
	    builder.field("user_min_timespan",user_min_timespan)

	    /* user_recency */
	    builder.field("user_recency",user_recency)

	    /* user_day_pref */
	    builder.startArray("user_day_pref")
	    for (rec <- user_day_pref) {
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

        /* user_time_pref */
	    builder.startArray("user_time_pref")
	    for (rec <- user_time_pref) {
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
	    
	    builder.endObject()
        
      })
      
    }).toList
    
  }

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
      val supp = x._2.map(_._2).foldLeft(0.toInt)(_ + _)
      
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