package de.kp.shopify.insight.actor.storage
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

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.shopify.insight.RequestContext

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.elastic._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * The PRMLoader stores the product relation model in the server layer,
 * i.e. in an Elasticsearch index.
 */
class PRMLoader(requestCtx:RequestContext) extends BaseActor {

  override def receive = {
   
    case message:StartLoad => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
        
        requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model load request received.""",uid)
        
        val store = String.format("""%s/PRM/%s""",requestCtx.getBase,uid)         
        val parquetFile = extract(store)

        requestCtx.listener ! String.format("""[INFO][UID: %s] Parquet file successfully retrieved.""",uid)
        
        val sources = transform(req_params,parquetFile)

        if (requestCtx.putSources("products","rules",sources) == false)
          throw new Exception("Loading process has been stopped due to an internal error.")

        requestCtx.listener ! String.format("""[INFO][UID: %s] Product relation model loading finished.""",uid)

        val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PRM")            
        context.parent ! LoadFinished(data)           
            
        context.stop(self)
         
      } catch {
        case e:Exception => {
                    
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Product relation model loading failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! LoadFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }

  private def extract(store:String):RDD[ParquetPRM] = {

    val sqlCtx = new SQLContext(requestCtx.sparkContext)
    import sqlCtx.createSchemaRDD
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlCtx.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val antecedent = data("antecedent").asInstanceOf[Seq[Int]]
      val consequent = data("consequent").asInstanceOf[Seq[Int]]
      
      val support = data("support").asInstanceOf[Int]
      val total = data("total").asInstanceOf[Long]

      val confidence = data("confidence").asInstanceOf[Double]
      
      ParquetPRM(antecedent,consequent,support,total,confidence)
      
    })

  }
  
  private def transform(params:Map[String,String],rules:RDD[ParquetPRM]):List[XContentBuilder] = {
            
    val uid = params(Names.REQ_UID)
    rules.map(rule => {
      
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))
      
      /* timestamp */
      builder.field(Names.TIMESTAMP_FIELD,params("timestamp"))

	  /* created_at_min */
	  builder.field("created_at_min",params("created_at_min"))

	  /* created_at_max */
	  builder.field("created_at_max",params("created_at_max"))
	  
	  /* antecedent */
	  builder.startArray("antecedent")
	  rule.antecedent.foreach(v => builder.value(v))
	  builder.endArray()
	  
	  /* consequent */
	  builder.startArray("consequent")
	  rule.antecedent.foreach(v => builder.value(v))
	  builder.endArray()

	  /* support */
	  builder.field("support",rule.support)
	  
	  /* total */
	  builder.field("total",rule.total)
	  
	  /* confidence */
	  builder.field("confidence",rule.confidence)
	  
	  builder.endObject()
      builder
      
    }).collect.toList
    
  }
  
}