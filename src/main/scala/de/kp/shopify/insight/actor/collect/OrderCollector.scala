package de.kp.shopify.insight.actor.collect
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

import org.apache.spark.SparkContext._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import org.joda.time.DateTime

import de.kp.spark.core.Names
import de.kp.spark.core.io._

import de.kp.shopify.insight._

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

class OrderCollector(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds

  override def receive = {

    case message:StartCollect => {
      
      val uid = params(Names.REQ_UID)
      
      try {
      
        ctx.listener ! String.format("""[INFO][UID: %s] Order base synchronization started.""",uid)
            
        val start = new java.util.Date().getTime            
        val orders = ctx.getOrders(params)
       
        ctx.listener ! String.format("""[INFO][UID: %s] Order base loaded.""",uid)

        /*
         * STEP #1: Write orders of a certain period of time 
         * to the database/orders index and 
         */
        writeOrders(params,orders)

        /*
         * STEP #2: Write aggregate information about monetary
         * and temporal data dimensions of a certain period of
         * time to the database/aggregates index
         */
        writeAggregate(params,orders)
        
        val end = new java.util.Date().getTime
        ctx.listener ! String.format("""[INFO][UID: %s] Order base synchronization finished in %s ms.""",uid,(end-start).toString)
        
        val new_params = Map(Names.REQ_MODEL -> "ORDER") ++ params

        context.parent ! CollectFinished(new_params)
        context.stop(self)
        
      } catch {
        case e:Exception => {

          ctx.listener ! String.format("""[ERROR][UID: %s] Order base synchronization failed due to an internal error.""",uid)
          
          val new_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ params

          context.parent ! CollectFailed(params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }
  
  private def writeAggregate(params:Map[String,String],orders:List[Order]) {

    val writer = new ElasticWriter()

    if (writer.open("database","aggregates") == false)
      throw new Exception("Aggregates database cannot be opened.")

    val source = buildAggregate(params,orders)
        
    writer.writeJSON("database", "aggregates", source)
    writer.close()
    
  }

  private def writeOrders(params:Map[String,String],orders:List[Order]) {

    val writer = new ElasticWriter()

    if (writer.open("database","orders") == false)
      throw new Exception("Order database cannot be opened.")

    val sources = orders.map(x=> buildOrder(params,x))
        
    writer.writeBulkJSON("database", "orders", sources)
    writer.close()
    
  }
  
  private def buildAggregate(params:Map[String,String],orders:List[Order]):XContentBuilder = {
     
    val uid = params(Names.REQ_UID)
    val total = orders.size
    /*
     * Extract the temporal and monetary dimension from the raw dataset
     */
    val orders_rfm = orders.map(order => (order.amount,order.timestamp))    
    
    /********************* MONETARY DIMENSION ********************/
    
    /*
     * Compute the average, minimum and maximum amount of all 
     * the purchases in the purchase history provided by orders
     */    
    val amounts = ctx.sparkContext.parallelize(orders_rfm.map(_._1))
    val m_stats = amounts.stats
    
    val m_mean  = m_stats.mean
    
    val m_min = m_stats.min
    val m_max = m_stats.max
    
    val m_stdev = m_stats.stdev
    val m_sum   = m_stats.sum
    
    val m_variance = m_stats.variance
    
    
    /********************* TEMPORAL DIMENSION ********************/
    
    /*
     * Compute the average,minimum and maximum time elapsed between 
     * two subsequent transactions; the 'zip' method is used to pairs 
     * between two subsequent timestamps
     */
    val timestamps = ctx.sparkContext.parallelize(orders_rfm.map(_._2)).sortBy(x => x)
    
    val s0 = ctx.sparkContext.parallelize(timestamps.take(1))
    val s1 = timestamps.subtract(s0)
    
    val timespans = timestamps.zip(s1).map(x => x._2 - x._1).map(v => (if (v / DAY < 1) 1 else v / DAY))
    val t_stats = timespans.stats
    
    val t_mean  = t_stats.mean
    
    val t_min = t_stats.min
    val t_max = t_stats.max
    
    val t_stdev = t_stats.stdev
    val t_variance = t_stats.variance

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
    val dow_supp = timestamps.map(x => new DateTime(x).dayOfWeek().get).groupBy(x => x).map(x => (x._1,x._2.size))
    val dow_pref = dow_supp.map(v => (v._1, Math.log(1 + v._2.toDouble / total.toDouble)))

    /* Time of day: 0..23 */
    val hod_supp = timestamps.map(x => new DateTime(x).hourOfDay().get).groupBy(x => x).map(x => (x._1,x._2.size))
    val hod_pref = hod_supp.map(v => (v._1, Math.log(1 + v._2.toDouble / total.toDouble)))
    
    /*********************** ITEM DIMENSION **********************/

     /*
     * Extract the item dimension from the raw dataset
     */
    val orders_itm = orders.flatMap(x => x.items.map(v => (v.item,v.quantity)))
    
    val itm_supp = orders_itm.groupBy(x => x._1).map(x => (x._1,x._2.map(_._2).sum))
    val itm_pref = itm_supp.map(v => (v._1, Math.log(1 + v._2.toDouble / total.toDouble)))
          
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
	builder.field("total_amount",m_sum)
	
	/* total_avg_amount */
	builder.field("total_avg_amount",m_mean)

	/* total_max_amount */
	builder.field("total_max_amount",m_max)

	/* total_min_amount */
	builder.field("total_min_amount",m_min)
	
	/* total_stdev_amount */
	builder.field("total_stdev_amount",m_stdev)

	/* total_variance_amount */
	builder.field("total_variance_amount",m_variance)

	/* total_avg_timespan */
	builder.field("total_avg_timespan",t_mean)

	/* total_max_timespan */
	builder.field("total_max_timespan",t_max)

	/* total_min_timespan */
	builder.field("total_min_timespan",t_min)
	
	/* total_stdev_timespan */
	builder.field("total_stdev_timespan",t_stdev)

	/* total_variance_timespan */
	builder.field("total_variance_timespan",t_variance)

	/* total_day_supp */
	builder.startArray("total_day_supp")
	for (rec <- dow_supp) {
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
	for (rec <- dow_pref) {
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
	for (rec <- hod_supp) {
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
	for (rec <- hod_pref) {
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
	for (rec <- itm_supp) {

	  builder.startObject()
          
	  builder.field("item",rec._1)
      builder.field("supp",rec._2)
              
      builder.endObject()
    }

    builder.endArray()
    
	/* total_item_pref */
	builder.startArray("total_item_pref")
	for (rec <- itm_pref) {

	  builder.startObject()
          
	  builder.field("item", rec._1)
      builder.field("score",rec._2)
              
      builder.endObject()
    }
    
    builder.endArray()
    
    builder.endObject()
    builder
    
  }
  
  private def buildOrder(params:Map[String,String],order:Order):XContentBuilder = {
    
    val uid = params("uid")
    val timestamp = params("timestamp").toLong
    /*
     * Note, that we must index the time period as timestamps
     * as these parameters are used to filter orders later on
     */
    val created_at_min = unformatted(params("created_at_min"))
    val created_at_max = unformatted(params("created_at_max"))
    
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
               
   /********** METADATA **********/
    
	/* uid */
	builder.field("uid",uid)
	
	/* last_sync */
	builder.field("last_sync",timestamp)
	
	/* created_at_min */
	builder.field("created_at_min",created_at_min)
	
	/* created_at_max */
	builder.field("created_at_max",created_at_max)
	
	/* site */
	builder.field("site",order.site)
             
    /********** ORDER DATA **********/
	
	/* user */
	builder.field("user",order.user)
	
	/* amount */
	builder.field("amount",order.amount)
	
	/* timestamp */
	builder.field("timestamp",order.timestamp)
	
	/* group */
	builder.field("group",order.group)
	
	/* ip_address */
	builder.field("ip_address",order.ip_address)
	
	/* user_agent */
	builder.field("user_agent",order.user_agent)
 	
	/* items */
	builder.startArray("items")
	
	for (item <- order.items) {
	  
	  builder.startObject()
	  
	  /* item */
	  builder.field("item",item.item)
	  
	  /* quantity */
	  builder.field("quantity",item.quantity)

	  /* category */
	  builder.field("category",item.category)

	  /* vendor */
	  builder.field("vendor",item.vendor)
	  
	  builder.endObject()
	  
	}
	
    builder.endArray()
	
	builder.endObject()	
	builder

  }

}