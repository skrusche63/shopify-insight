package de.kp.shopify.insight.actor.query

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
            
            val filters = Buffer.empty[FilterBuilder]
            /*
             * This method retrieves the forecast description of a certain user,
             * specified by 'site' and 'user' identifier 
             */
            val site = req_params(Names.REQ_SITE)
            filters += FilterBuilders.termFilter(Names.REQ_SITE, site)
            
            val user = req_params(Names.REQ_USER)
            filters += FilterBuilders.termFilter(Names.REQ_USER, user)
    
            if (req_params.contains(Names.REQ_UID))
              filters += FilterBuilders.termFilter(Names.REQ_UID, req_params(Names.REQ_UID))
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
            
            val forecasts = ESQuestor.query_FilteredForecasts(requestCtx,fbuilder)
            origin ! InsightForecasts(forecasts)
        
            context.stop(self)
            
          }
          
          case "user_loyalty" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty request received.""",uid)
            
            val filters = Buffer.empty[FilterBuilder]
            /*
             * This method retrieves the loyalty description of a certain user,
             * specified by 'site' and 'user' identifier 
             */
            val site = req_params(Names.REQ_SITE)
            filters += FilterBuilders.termFilter(Names.REQ_SITE, site)
            
            val user = req_params(Names.REQ_USER)
            filters += FilterBuilders.termFilter(Names.REQ_USER, user)
    
            if (req_params.contains(Names.REQ_UID))
              filters += FilterBuilders.termFilter(Names.REQ_UID, req_params(Names.REQ_UID))
            
            val fbuilder = FilterBuilders.boolFilter()
            fbuilder.must(filters:_*)
            
            val loyalties = ESQuestor.query_FilteredLoyalties(requestCtx,fbuilder)
            origin ! InsightLoyalties(loyalties)
        
            context.stop(self)
            
          }
          
          case "user_recommendation" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] User recommendation request received.""",uid)

            val total = if (req_params.contains(Names.REQ_TOTAL)) req_params(Names.REQ_TOTAL).toInt else 10
            val filters = Buffer.empty[FilterBuilder]
            /*
             * This method retrieves the forecast description of a certain user,
             * specified by 'site' and 'user' identifier 
             */
            val site = req_params(Names.REQ_SITE)
            filters += FilterBuilders.termFilter(Names.REQ_SITE, site)
            
            val user = req_params(Names.REQ_USER)
            filters += FilterBuilders.termFilter(Names.REQ_USER, user)
    
            if (req_params.contains(Names.REQ_UID))
              filters += FilterBuilders.termFilter(Names.REQ_UID, req_params(Names.REQ_UID))
            
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
 
}