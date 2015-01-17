package de.kp.shopify.insight.actor.query
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

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.search.SearchResponse

import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.actor.BaseActor

import de.kp.shopify.elastic._
import de.kp.shopify.insight.model._

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

class ProductQuestor(requestCtx:RequestContext) extends BaseActor(requestCtx) {
  
  private val COVERAGE_THRESHOLD = 0.25
  
  override def receive = {

    case query:ProductQuery => {

      val req_params = query.data
      val uid = req_params(Names.REQ_UID)
      
      val origin = sender
      try {
       
        val method = req_params(Names.REQ_METHOD)
        method match {
          
          case "product_cross_sell" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] Product cross sell request received.""",uid)
 
            val items = query.data(Names.REQ_ITEMS).split(",")
            val total = if (req_params.contains(Names.REQ_TOTAL)) req_params(Names.REQ_TOTAL).toInt else 10
            /*
             * Retrieve the product rules of a certain time span,
             * identified by the unique task identifier (uid)
             */
            val rules = ESQuestor.query_Rules(requestCtx, uid)
        
//            /*
//             * Return sorted list consequent products with the highest overlap with 
//             * the 'antecedent' part of the respective rules, the highest 'confidence' 
//             * and 'support' 
//             */
//            val candidates = rules.map(rule => {
//
//              val antecedent = rule.antecedent
//              val consequent = rule.consequent
//        
//              val support = rule.support.toDouble / rule.total
//              val confidence = rule.confidence
//       
//              val count = consequent.size
//              val coverage = items.intersect(antecedent).size.toDouble / antecedent.size
//       
//              (consequent,support,confidence,coverage,count)
//       
//            }).filter(x => x._5 > COVERAGE_THRESHOLD).map(x => (x._1, x._2 * x._3 * x._4, x._5))           
//            
//            val total_count = candidates.map(_._3).sum
//            val consequent = if (total < total_count) {
//              
//              val items = Buffer.empty[ItemPref]
//              candidates.sortBy(x => -x._2).foreach(x => {
//                
//                val rest = total - items.size
//                if (rest > x._3) x._1.foreach(v => items += ItemPref(v,x._2))
//                else {
//                  x._1.take(rest).foreach(v => items += ItemPref(v,x._2))
//                }                
//              })
//              
//              items.toList
//              
//            } else {
//              /*
//               * We have to take all candidates into account, 
//               * as the request requiers more or equal that 
//               * are available
//               */
//              candidates.sortBy(x => -x._3).flatMap(x => x._1.map(v => ItemPref(v,x._2)))
//            }
//            
//            /*
//             * Retrieve metadata from first rule
//             */
//            val head = rules.head
//            val (timestamp,created_at_min,created_at_max) = (head.timestamp,head.created_at_min,head.created_at_max)
//
//            val result = InsightFilteredItems(
//                uid,
//                timestamp,
//                created_at_min,
//                created_at_max,
//                consequent.size,
//                consequent.toList
//             )
//            
//            origin ! result
            context.stop(self)
            
          }
          
          case "product_promotion" => {
             
            requestCtx.listener ! String.format("""[INFO][UID: %s] Product promotion request received.""",uid)
 
            val items = query.data(Names.REQ_ITEMS).split(",")
            val total = if (req_params.contains(Names.REQ_TOTAL)) req_params(Names.REQ_TOTAL).toInt else 10
            /*
             * Retrieve the product rules of a certain time span,
             * identified by the unique task identifier (uid)
             */
            val rules = ESQuestor.query_Rules(requestCtx, uid)
        
//            /*
//             * Return sorted list antecedent products with the highest overlap with 
//             * the 'consequent' part of the respective rules, the highest 'confidence' 
//             * and 'support' 
//             */
//            val candidates = rules.map(rule => {
//
//              val antecedent = rule.antecedent
//              val consequent = rule.consequent
//        
//              val support = rule.support.toDouble / rule.total
//              val confidence = rule.confidence
//       
//              val count = consequent.size
//              val coverage = items.intersect(consequent).size.toDouble / consequent.size
//       
//              (antecedent,support,confidence,coverage,count)
//       
//            }).filter(x => x._5 > COVERAGE_THRESHOLD).map(x => (x._1, x._2 * x._3 * x._4, x._5))           
//            
//            val total_count = candidates.map(_._3).sum
//            val antecedent = if (total < total_count) {
//              
//              val items = Buffer.empty[ItemPref]
//              candidates.sortBy(x => -x._2).foreach(x => {
//                
//                val rest = total - items.size
//                if (rest > x._3) x._1.foreach(v => items += ItemPref(v,x._2))
//                else {
//                  x._1.take(rest).foreach(v => items += ItemPref(v,x._2))
//                }                
//              })
//              
//              items.toList
//              
//            } else {
//              /*
//               * We have to take all candidates into account, 
//               * as the request requiers more or equal that 
//               * are available
//               */
//              candidates.sortBy(x => -x._3).flatMap(x => x._1.map(v => ItemPref(v,x._2)))
//            }
//            
//            /*
//             * Retrieve metadata from first rule
//             */
//            val head = rules.head
//            val (timestamp,created_at_min,created_at_max) = (head.timestamp,head.created_at_min,head.created_at_max)
//
//            val result = InsightFilteredItems(
//                uid,
//                timestamp,
//                created_at_min,
//                created_at_max,
//                antecedent.size,
//                antecedent.toList
//             )
//            
//            origin ! result
            context.stop(self)
           
          }
          
          case "product_suggest" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] Product suggest request received.""",uid)
            
            val total = if (req_params.contains(Names.REQ_TOTAL)) req_params(Names.REQ_TOTAL).toInt else 10
            /*
             * Retrieve the product rules of a certain time span,
             * identified by the unique task identifier (uid)
             */
            val rules = ESQuestor.query_Rules(requestCtx, uid)
        
//            /*
//             * Return sorted list antecedent products with the highest overlap with 
//             * the 'consequent' part of the respective rules, the highest 'confidence' 
//             * and 'support' 
//             */
//            val candidates = rules.map(rule => {
//
//              val items = rule.antecedent ++ rule.consequent
//        
//              val support = rule.support.toDouble / rule.total
//              val confidence = rule.confidence
//       
//              val count = items.size      
//              (items,support,confidence,count)
//       
//            }).map(x => (x._1, x._2 * x._3, x._4))          
//            
//            val total_count = candidates.map(_._3).sum
//            
//            val suggestion = if (total < total_count) {
//              
//              val items = Buffer.empty[ItemPref]
//              candidates.sortBy(x => -x._2).foreach(x => {
//                
//                val rest = total - items.size
//                if (rest > x._3) x._1.foreach(v => items += ItemPref(v,x._2))
//                else {
//                  x._1.take(rest).foreach(v => items += ItemPref(v,x._2))
//                }                
//              })
//              
//              items.toList
//              
//            } else {
//              /*
//               * We have to take all candidates into account, 
//               * as the request requiers more or equal that 
//               * are available
//               */
//              candidates.sortBy(x => -x._3).flatMap(x => x._1.map(v => ItemPref(v,x._2)))
//            }
//            
//            /*
//             * Retrieve metadata from first rule
//             */
//            val head = rules.head
//            val (timestamp,created_at_min,created_at_max) = (head.timestamp,head.created_at_min,head.created_at_max)
//
//            val result = InsightFilteredItems(
//                uid,
//                timestamp,
//                created_at_min,
//                created_at_max,
//                suggestion.size,
//                suggestion.toList
//             )
//            
//            origin ! result
            context.stop(self)
            
          }
          
          case "product_top_sell" => {
            
            requestCtx.listener ! String.format("""[INFO][UID: %s] Product top sell request received.""",uid)
            
            /*
             * This method retrieves the top selling products from the orders/aggregates
             * index and extracts those products with the highest support
             */
            val average = ESQuestor.query_Aggregate(requestCtx, uid)
            val total_item_supp = average.total_item_supp
            
            val total = if (req_params.contains(Names.REQ_TOTAL)) req_params(Names.REQ_TOTAL).toInt else 10
            val sorted = total_item_supp.sortBy(x => -x.supp)

            val top_item_supp = if (total < sorted.size) sorted.take(total) else sorted            
//            val result = InsightTopItems(
//                average.uid,
//                average.timestamp,
//                average.created_at_min,
//                average.created_at_max,
//                top_item_supp.size,
//                top_item_supp
//            )
//            
//            origin ! result
            context.stop(self)
            
          }
          
          case _ => throw new Exception("The request method '" + method + "' is not supported.")
          
        }
      
      } catch {
        case e:Exception => {
            
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Product query failed: %s.""",uid,e.getMessage)

          val created_at_min = req_params("created_at_min")
          val created_at_max = req_params("created_at_max")

          origin ! SimpleResponse(uid,created_at_min,created_at_max,e.getMessage)
          
          context.stop(self)
          
        }
      
      }
      
    }
  
  }  

}