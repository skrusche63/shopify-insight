package de.kp.shopify.insight.actor.query

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.actor.BaseActor

import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.model._

import scala.collection.mutable.ArrayBuffer

import org.elasticsearch.index.query._
import scala.collection.mutable.Buffer

class UserQuestor(requestCtx:RequestContext) extends BaseActor {

  def receive = {
    
    case query:UserQuery => {

      val req_params = query.data
      val uid = req_params(Names.REQ_UID)
      
      val origin = sender
      try {
       
        val method = req_params(Names.REQ_METHOD)
        method match {

          case "user_forecast" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] User forecast request received.""",uid)
            
            val filters = buildUserFilters(req_params)
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
            
            val forecasts = ESQuestor.query_FilteredForecasts(requestCtx,fbuilder)
            origin ! InsightForecasts(forecasts)
        
            context.stop(self)
            
          }
          
          case "user_loyalty" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty request received.""",uid)
            
            val filters = buildUserFilters(req_params)
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
            
            val loyalties = ESQuestor.query_FilteredLoyalties(requestCtx,fbuilder)
            origin ! InsightLoyalties(loyalties)
        
            context.stop(self)
            
          }
          
          case "user_next_purchase" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] User next purchase request received.""",uid)

            /*
             * Retrieve all (user) forecasts that specify purchase events from today within
             * the next 'n' days; we sort these events by the probability score provided and
             * return a list with 'user', 'timestamp' and 'probability
             */
            val days = if (req_params.contains(Names.REQ_DAYS)) req_params(Names.REQ_DAYS).toInt else 30
    
            val today = new DateTime()
            
            val from = today.getMillis
            val to = today.plusDays(days).getMillis
            
            val filters = buildTimespanFilters(from,to)
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
            
            val forecasts = ESQuestor.query_FilteredForecasts(requestCtx,fbuilder)
            /*
             * Build from these forecasts a group of users and specify their
             * predicted purchase timestamp and the respective score
             */
            val users = forecasts.groupBy(x => (x.site,x.user)).map(f => {
              
              val (site,user) = f._1
              /*
               * Note, that we have computed more than one step into 
               * the future, but due to conditional probability, we actually
               * only take the highest score per user
               */
              val data = f._2.sortBy(x => -x.score).map(x => (x.time,x.score)).head
              
              InsightPurchaseSegmentUser(site,user,data._1,data._2)
              
            }).toList
            
            origin ! InsightPurchaseSegment(from,to,users)
            context.stop(self)

          }
          
          case "user_recommendation" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] User recommendation request received.""",uid)

            val total = if (req_params.contains(Names.REQ_TOTAL)) req_params(Names.REQ_TOTAL).toInt else 10
            val filters = buildUserFilters(req_params)
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
 
            val response = ESQuestor.query_FilteredRecommendations(requestCtx, fbuilder)
            if (response.size != 1) throw new Exception("Illegal number of user recommendations detected.")
            
            val record = response(0)
            val candidates = record.recommendations.map(x => (x.consequent,x.score,x.consequent.size))
            
            val total_count = candidates.map(_._3).sum            
            val consequent = if (total < total_count) {
           
              val items = Buffer.empty[ItemPref]
              candidates.sortBy(x => -x._2).foreach(x => {
                
                val rest = total - items.size
                if (rest > x._3) x._1.foreach(v => items += ItemPref(v,x._2))
                else {
                  x._1.take(rest).foreach(v => items += ItemPref(v,x._2))
                }                
              })
              
              items.toList
              
            } else {
              /*
               * We have to take all candidates into account, 
               * as the request requires more or equal that 
               * are available
               */
              candidates.sortBy(x => -x._2).flatMap(x => x._1.map(v => ItemPref(v,x._2)))
            }
            
            /*
             * Retrieve metadata from record
             */
            val (timestamp,created_at_min,created_at_max) = (record.timestamp,record.created_at_min,record.created_at_max)

            val result = InsightFilteredItems(
                uid,
                timestamp,
                created_at_min,
                created_at_max,
                consequent.size,
                consequent.toList
             )
            
            origin ! result
            context.stop(self)
            
          }
          
          case _ => throw new Exception("The request method '" + method + "' is not supported.")
          
        }
      
      } catch {
        case e:Exception => {
            
          requestCtx.listener ! String.format("""[ERROR][UID: %s] User query failed: %s.""",uid,e.getMessage)
          origin ! SimpleResponse(uid,e.getMessage)
          
          context.stop(self)
          
        }
      
      }
      
    }
    
    case _ => 
      
  }
 
  private def buildTimespanFilters(params:Map[String,String]):List[FilterBuilder] = {

    val days = if (params.contains(Names.REQ_DAYS)) params(Names.REQ_DAYS).toInt else 30
    
    val today = new DateTime()
    
    val from = today.getMillis
    val to = today.plusDays(days).getMillis
            
    buildTimespanFilters(from,to)
    
  }
  private def buildTimespanFilters(from:Long,to:Long):List[FilterBuilder] = {
            
    val filters = Buffer.empty[FilterBuilder]
    filters += FilterBuilders.rangeFilter("time").from(from).to(to)
    
    filters.toList
    
  }

  private def buildUserFilters(params:Map[String,String]):List[FilterBuilder] = {
             
    val filters = Buffer.empty[FilterBuilder]
    /*
     * This method retrieves the forecast description of a certain user,
     * specified by 'site' and 'user' identifier 
     */
    val site = params(Names.REQ_SITE)
    filters += FilterBuilders.termFilter(Names.REQ_SITE, site)
            
    val user = params(Names.REQ_USER)
    filters += FilterBuilders.termFilter(Names.REQ_USER, user)
    
    if (params.contains(Names.REQ_UID))
      filters += FilterBuilders.termFilter(Names.REQ_UID, params(Names.REQ_UID))
   
    filters.toList
    
  }
}