package de.kp.shopify.insight.actor
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
import de.kp.shopify.insight.model._

import scala.collection.JavaConversions._

class ProductQuestor(requestCtx:RequestContext) extends BaseActor {
  
  override def receive = {

    case query:SuggestQuery => {
      
      val origin = sender
      try {
      
        val response = getElasticRules(query.data)
        /*
         * Transform search result list of frequent items 
         */
        val hits = response.getHits()
        val total = hits.totalHits()

        val freqitems = hits.hits().map(hit => {
        
          val data = hit.getSource()
          /*
           * Join antecedent & consequent items into a single list and return 
           * weighted list of most frequent products; to this end, we have to
           * access the REST API
           */
          val items = data(Names.ANTECEDENT_FIELD).asInstanceOf[List[Int]] ++ data(Names.CONSEQUENT_FIELD).asInstanceOf[List[Int]]
        
          val support = data(Names.SUPPORT_FIELD).asInstanceOf[Int]
          val confidence = data(Names.CONFIDENCE_FIELD).asInstanceOf[Double]
       
          val total = data(Names.TOTAL_FIELD).asInstanceOf[Long]
       
          /*
           * Return a list of most frequent items 
           */
          (items,support,total,confidence)
       
        })
        /*
         * Convert product identifier into product descriptions
         * that can be directly used to feed a certain widget
         */
      
        // TODO
        
      } catch {
        case e:Exception => {
          // TODO
        }
      }
      
    }
    
    case query:CrossSellQuery => {
      
      val origin = sender
      try {
        
        val items = query.data(Names.REQ_ITEMS).split(",")
        val response = getElasticRules(query.data)
         
        val hits = response.getHits()
        val total = hits.totalHits()
        
        /*
         * Return sorted list consequent products with the highest
         * overlap with the 'antecedent' part of the respective rules,
         * the highest 'confidence' and 'support' 
         */
        val products = hits.hits().map(hit => {
        
          val data = hit.getSource()

          val antecedent = data(Names.ANTECEDENT_FIELD).asInstanceOf[List[Int]]
          val consequent = data(Names.CONSEQUENT_FIELD).asInstanceOf[List[Int]]
        
          val support = data(Names.SUPPORT_FIELD).asInstanceOf[Int]
          val confidence = data(Names.CONFIDENCE_FIELD).asInstanceOf[Double]
       
          val ratio = items.intersect(antecedent).size.toDouble / antecedent.size
       
          (consequent,support,confidence,ratio)
       
        }).sortBy(x => (-x._4,-x._3,-x._2))
      
        /*
         * From the products determined, we compute a predefined set
         * of products that are candidates for cross-sell suggestions 
         */
        // TODO
      
      
      } catch {
        case e:Exception => {
          // TODO
        }
      }
      
    }
    
    case query:PromotionQuery => {
      
      val origin = sender
      try {
        
        val items = query.data(Names.REQ_ITEMS).split(",")
        val response = getElasticRules(query.data)

        val hits = response.getHits()
        val total = hits.totalHits()
        /*
         * Return sorted list antecedent products with the highest
         * overlap with the 'consequent' part of the respective rules,
         * the highest 'confidence' and 'support' 
         */
        val products = hits.hits().map(hit => {
        
          val data = hit.getSource()

          val antecedent = data(Names.ANTECEDENT_FIELD).asInstanceOf[List[Int]]
          val consequent = data(Names.CONSEQUENT_FIELD).asInstanceOf[List[Int]]
        
          val support = data(Names.SUPPORT_FIELD).asInstanceOf[Int]
          val confidence = data(Names.CONFIDENCE_FIELD).asInstanceOf[Double]
       
          val ratio = items.intersect(consequent).size.toDouble / consequent.size
       
          (antecedent,support,confidence,ratio)
       
        }).sortBy(x => (-x._4,-x._3,-x._2))
      
        /*
         * From the products determined, we compute a predefined set
         * of products that are candidates for promotion suggestions 
         */
        // TODO
       
      } catch {
        case e:Exception => {
          // TODO
        }
      }
      
    }
    case _ => 
      
  }
  
  private def getElasticRules(params:Map[String,String]):SearchResponse = {
        
    /*
     * Retrieve the association rules built before from the Elasticsearch 'product/rules' index;
     * the only parameter that is required to retrieve the data is 'uid'
     */
    val qbuilder = QueryBuilders.matchQuery(Names.UID_FIELD, params(Names.REQ_UID))
    requestCtx.find("products", "rules", qbuilder)
    
  }
  
}