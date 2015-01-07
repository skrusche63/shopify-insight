package de.kp.shopify.insight.actor.analyze
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
import org.apache.spark.sql.SQLContext

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.io.ParquetReader

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.actor.BaseActor

/**
 * The RECAnalyzer evaluates the RFM table created by the RFMPreparer
 * based on the quantiles computed before and registers the result as
 * a Parquet file
 */
class RECAnalyzer(requestCtx:RequestContext) extends BaseActor {
  
  override def receive = {
    
    case msg:StartAnalyze => {

      val req_params = msg.data      
      val uid = req_params(Names.REQ_UID)
      
      try {

        val sc = requestCtx.sparkContext
        val table = evaluate(req_params)
        
        val sqlCtx = new SQLContext(sc)
        import sqlCtx.createSchemaRDD

        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/REC/%s""",requestCtx.getBase,uid)         
        table.saveAsParquetFile(store)

        val params = Map(Names.REQ_MODEL -> "REC") ++ req_params
        context.parent ! PrepareFinished(params)
        
      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] REC analysis exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }

  private def evaluate(params:Map[String,String]):RDD[ParquetREC] = {
    
    val sc = requestCtx.sparkContext
    
    val uid = params(Names.REQ_UID)
    val store = String.format("""%s/RFM/%s""",requestCtx.getBase,uid)         

    val rawset = new ParquetReader(sc).read(store)
    /*
     * STEP #1 Retrieve quantile description from the first
     * entry of the dataset as this specification is normalized
     */
    val quantiles = sc.broadcast(rawset.take(1)(0)("R_Quantiles").asInstanceOf[String].split(",").map(v => {

        val Array(v0,v1) = v.split(":")
        
        val quant = v0.toDouble
        val value = v1.toDouble
        
        (quant,value)
      
      }).toMap)
    
    rawset.map(x => {
      
      val site = x("site").asInstanceOf[String]
      val user = x("user").asInstanceOf[String]
      
      val days = x("R").asInstanceOf[Int]
      /*
       * We use the quantiles description to divide
       * all the users into three segments:
       * 
       * a) high value::    0 < value < 0.25 boundary
       * b) medium value:: 0.25 boundary <= value < 0.75 boundary
       * c) low value::   0.75 boundary <= value
       */
      val b1 = quantiles.value(0.25)
      val b2 = quantiles.value(0.75)
      
      val state = (
          if (days < b1) "H"
          else if (b1 <= days && days < b2) "M"
          else if (b2 <= days) "L"
          else "-"  
      )
      
      ParquetREC(site,user,days,state)
      
    })
    
  }

}