package de.kp.shopify.elastic.plugin
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

import org.elasticsearch.client.Client

import org.elasticsearch.index.query._
import scala.collection.mutable.Buffer

class RecommendQuestor(client:Client) extends PredictiveQuestor(client) {

  private val CSM_INDEX = "customers"
  private val RECOM_MAPPING = "recommendations"

  private val PRM_INDEX = "products"
  private val REL_MAPPING = "relations"
  
  def recommended_products(site:String,user:String,created_at:Long):String = {
    /*
     * STEP #1: Build filtered query with 'site' and
     * 'user' parameter as filtered terms
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.termFilter("user", user)
    
    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  
    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all
     */
    val total = count(CSM_INDEX,RECOM_MAPPING,qbuilder)    
    val response = find(CSM_INDEX, RECOM_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    val result = if (total_hits == 0) {
      EsCPRList(List.empty[EsCPR])
      
    } else {    
      EsCPRList(hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsCPR])).toList)   
      
    }
    
    JSON_MAPPER.writerWithType(classOf[EsCPRList]).writeValueAsString(result)
     
  }

  def related_products(site:String,customer:Int,products:List[Int],created_at:Long):String = {
    /*
     * STEP #1: Build filtered query with 'site' parameter as filtered terms
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.termFilter("customer_type", customer)

    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  

    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all product relation rules that match
     * the provided parameters
     */
    val total = count(CSM_INDEX,RECOM_MAPPING,qbuilder)    
    val response = find(CSM_INDEX, RECOM_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    val result = if (total_hits == 0) {
      EsBPRList(List.empty[EsBPR])
      
    } else {    
      /*
       * STEP #3: Score the consequent part of the respective rule by
       * 
       * a) the overlap ratio of the antecedent part with the provided 
       *    products, 
       *    
       * b) the confidence and 
       * 
       * c) the support ratio
       * 
       */
      val rules = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsPRM]))
      EsBPRList(rules.map(rule => {
        
        val ratio = rule.support.toDouble / rule.total
        
        val antecedent = rule.antecedent        
        val weight = products.intersect(antecedent).size.toDouble / antecedent.size
        
        val score = weight * rule.confidence * ratio
        
        EsBPR(rule.uid,rule.timestamp,rule.site,rule.consequent,score,rule.customer_type)
      
      }).toList)
      
    }
    
    JSON_MAPPER.writerWithType(classOf[EsBPRList]).writeValueAsString(result)
    
  }
  
}