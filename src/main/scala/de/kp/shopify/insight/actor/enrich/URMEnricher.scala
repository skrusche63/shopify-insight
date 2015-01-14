package de.kp.shopify.insight.actor.enrich
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
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import de.kp.spark.core.Names
import de.kp.spark.core.model._
import de.kp.shopify.insight.RequestContext
import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._
import de.kp.shopify.insight.elastic._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}
import de.kp.shopify.insight.build._

/**
 * URMEnricher is an actor that uses an association rule model, 
 * transforms the model into a user recommendation model and and 
 * registers the result as a Parquet file.
 */
class URMEnricher(requestCtx:RequestContext) extends BaseActor(requestCtx) {
  
  private val COVERAGE_THRESHOLD = 0.25
  import sqlc.createSchemaRDD

  override def receive = {
   
    case message:StartEnrich => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
        
        requestCtx.listener ! String.format("""[INFO][UID: %s] User recommendation model building started.""",uid)
        
        /*
         * STEP #1: Load Parquet file with previously build customer state description
         */
        val store = String.format("""%s/ASR/%s""",requestCtx.getBase,uid)         
        val parquetItems = readParquetItems(store)
         
        /*
         * STEP #2: Retrieve association rules from the Association Analysis engine
         */      
        val (service,request) = buildRemoteRequest(req_params)

        val response = requestCtx.getRemoteContext.send(service,request).mapTo[String]            
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {
              /*
               * STEP #3: Build product recommendations by merging the association
               * rules and the last transaction itemsets
               */            
              val table = buildTable(res,parquetItems)
              /* 
               * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
               * allowing it to be stored using Parquet. 
               */
              val store = String.format("""%s/URM/%s""",requestCtx.getBase,uid)         
              table.saveAsParquetFile(store)

              requestCtx.listener ! String.format("""[INFO][UID: %s] User recommendation model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "URM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
             
            }
            
          }
          
        }
        /*
         * The Association Analysis engine returned an error message
         */
        response.onFailure {
          case throwable => {
                    
            requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
         
      } catch {
        case e:Exception => {
                    
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }

  private def buildRemoteRequest(params:Map[String,String]):(String,String) = {
    /*
     * Product recommendations are derived from the assocation rule model;
     * note, that we do not leverage the 'transaction' channel here.
     */
    val service = "association"
    val task = "get:rule"

    val data = new AAEHandler().get(params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  
  private def buildTable(response:ServiceResponse,itemsets:RDD[(String,String,List[Int])]):RDD[ParquetURM] = {
   
    val threshold = sc.broadcast(COVERAGE_THRESHOLD)
    val rules = sc.broadcast(Serializer.deserializeRules(response.data(Names.REQ_RESPONSE)))
  
    itemsets.map(itemset => {
      
      val (site,user,items) = itemset
      /*
       * Replace antecendent part of the retrieved rules by the itemset of the last 
       * user transaction, compute intersection ratio and restrict to those modified 
       * rules where antecedent and consequent are completely disjunct; finally sort
       * new rules by a) ratio AND b) confidence AND c) support and take best element 
       */
      val new_rules = rules.value.items.map(rule => {

        val support = rule.support.toDouble / rule.total
        val coverage = items.intersect(rule.antecedent).length.toDouble / items.length
        
        (items,rule.consequent,support,rule.confidence,coverage)
      
      }).filter(r => (r._1.intersect(r._2).size > 0) && r._5 > threshold.value)
      /* 
       * Build recommendation score for each rule from support, confidence and coverage,
       * and find best rule with highest score 
       */
      val sorted_rules = new_rules.map(x => (x._1,x._2,x._3 * x._4 * x._5)).sortBy(x => -x._3)
      val recommendations = sorted_rules.map(x => (x._2.toSeq,x._3)).toSeq
      
      ParquetURM(site,user,recommendations)

    })
 
  }
  /**
   * This method reads the Parquet file that specifies the item representation 
   * for all customers that have purchased since the start of the collection 
   * of the Shopify orders.
   */
  private def readParquetItems(store:String):RDD[(String,String,List[Int])] = {
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    val rawset = parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val group = data("group").asInstanceOf[String]
      val item = data("item").asInstanceOf[Int]
      
      (site,user,group,item)
      
    })
    
    rawset.groupBy(x => (x._1,x._2)).map(p => {

      val (site,user) = p._1
      /*
       * Constrain to the last two transactions and retrieve
       * the respective timestamp and amount
       */
      val items = p._2.map(_._4).toList
      (site,user,items)
      
    })

  }  
}