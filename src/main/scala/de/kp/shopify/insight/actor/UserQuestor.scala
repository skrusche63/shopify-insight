package de.kp.shopify.insight.actor

import org.elasticsearch.index.query.QueryBuilders

import de.kp.spark.core.Names

import de.kp.shopify.insight.FindContext
import de.kp.shopify.insight.model._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class UserQuestor(findContext:FindContext) extends BaseActor {
        
  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds

  def receive = {
    
    case query:ForecastQuery => {
      
      val origin = sender
      try {
      
        /*
         * Retrieve the forecasts built before from the Elasticsearch 'orders/forecasts' index;
         * the only parameter that is required to retrieve the rules is 'uid'
         */
        val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, query.data(Names.REQ_UID))
        val response = findContext.find("orders", "forecasts", qbuilder)
        /*
         * Transform search result list of frequent items 
         */
        val hits = response.getHits()
        val total = hits.totalHits()
      
        val now = new java.util.Date()
        val timestamp = now.getTime()
      
        /*
         * Rebuild forecasts from search results and filter those forecasts that
         * refer to timestamps in the future; note, that time evaluation is based
         * on the current time, while training uses the last transaction date
         */
        val rawset = hits.hits().map(hit => {
        
          val data = hit.getSource()
        
          val site = data(Names.SITE_FIELD).asInstanceOf[String]
          val user = data(Names.USER_FIELD).asInstanceOf[String]
        
          val step = data(Names.STEP_FIELD).asInstanceOf[Int]

          val amount = data(Names.AMOUNT_FIELD).asInstanceOf[Float]
          val timestamp = data(Names.TIMESTAMP_FIELD).asInstanceOf[Long]

          val score = data(Names.SCORE_FIELD).asInstanceOf[Double]      

          (site,user,step,amount,timestamp,score)
      
        }).filter(x => x._5 > timestamp)

        val forecasts = buildForecasts(rawset)      
        origin ! forecasts
      
      } catch {
        case e:Exception => {
          // TODO
        }
      }
      
    }
    
    case _ => 
      
  }
  
  private def buildForecasts(rawset:Array[(String,String,Int,Float,Long,Double)]):Forecasts = {
      
    val now = new java.util.Date()
    val timestamp = now.getTime()
      
    val forecasts = rawset.groupBy(x => (x._1,x._2)).flatMap(g => {
        
      val (site,user) = g._1
      val data = g._2.sortBy(x => x._3)
        
      val events = ArrayBuffer.empty[Forecast]
        
      val head = data.head
      /*
       * 'amount' & 'score' refer to the individual step
       * under consideration; timestamp is the absolute
       * timestamp calculated from the last purchase
       */
      val (_amount,_timestamp,_score) = (head._4,head._5,head._6)
      /*
       * Convert timestamp into day period from today; note, that this
       * reconstructs the pre-defined time horizons such as 15, 45 or 90
       */
      val _days = ((_timestamp - timestamp) / DAY).toInt        
        
      var pre_amount = _amount
      var pre_score  = _score
          
      events += Forecast(site,user,pre_amount,_days,pre_score)
        
      for (record <- data.tail) {
          
        val (_amount,_timestamp,_score) = (record._4,record._5,record._6)
        val _days = ((_timestamp - timestamp) / DAY).toInt
          
        /*
         * The next step is described by the total number of days from
         * today and the aggregated amount and score
         */
        pre_amount = pre_amount + _amount
        pre_score  = pre_score * _score
          
        events += Forecast(site,user,pre_amount,_days,pre_score)
          
      }
      
      events.toList
    
    }).toList
  
    Forecasts(forecasts)
    
  }
  
}