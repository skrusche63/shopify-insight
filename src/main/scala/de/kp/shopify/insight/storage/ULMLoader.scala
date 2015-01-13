package de.kp.shopify.insight.storage
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

import de.kp.shopify.insight.actor.BaseActor
import de.kp.shopify.insight.model._

import de.kp.shopify.insight.elastic._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * The ULMLoader stores the user loyalty model in the server layer,
 * i.e. in an Elasticsearch index.
 */
class ULMLoader(requestCtx:RequestContext) extends BaseActor(requestCtx) {

  override def receive = {
   
    case message:StartLoad => {
      
      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty model load request received.""",uid)
        
        val store = String.format("""%s/ULM/%s""",requestCtx.getBase,uid)         
        val parquetFile = extract(store)
      
        requestCtx.listener ! String.format("""[INFO][UID: %s] Parquet file successfully retrieved.""",uid)

        val sources = transform(req_params,parquetFile)
            
        if (requestCtx.putSources("users","loyalties",sources) == false)
          throw new Exception("Loading process has been stopped due to an internal error.")

        requestCtx.listener ! String.format("""[INFO][UID: %s] User loyalty model loading finished.""",uid)

        val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "ULM")            
        context.parent ! LoadFinished(data)           
            
        context.stop(self)
         
      } catch {
        case e:Exception => {
                    
          requestCtx.listener ! String.format("""[ERROR][UID: %s] User loyalty model loading failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! LoadFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }

  }  

  private def transform(params:Map[String,String],loyalty:RDD[ParquetULM]):List[XContentBuilder] = {
    
    loyalty.map(x => {
          
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
       
      /********** METADATA **********/
        
      /* uid */
      builder.field("uid",params("uid"))
       
      /* timestamp */
      builder.field("timestamp",params("timestamp"))

	  /* created_at_min */
	  builder.field("created_at_min",params("created_at_min"))

	  /* created_at_max */
	  builder.field("created_at_max",params("created_at_max"))
      
      /* site */
      builder.field("site",x.site)
      
      /********** LOYALTY DATA **********/
      
      /* user */
      builder.field("user",x.user)
      
      /* trajectory */
	  builder.startArray("trajectory")
	  x.trajectory.foreach(v => builder.value(v))
	  builder.endArray()
      
      /* low */
      builder.field("low",x.low)
      
      /* norm */
      builder.field("norm",x.norm)
      
      /* high */
      builder.field("high",x.high)
      
      /* rating */
      builder.field("rating",x.rating)
      
      builder.endObject()
      builder
      
    }).collect.toList
  
  }
 
  private def extract(store:String):RDD[ParquetULM] = {
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val trajectory = data("trajectory").asInstanceOf[Seq[String]]
      
      val low = data("low").asInstanceOf[Double]
      val norm = data("norm").asInstanceOf[Double]

      val high = data("high").asInstanceOf[Double]
      val rating = data("rating").asInstanceOf[Int]

      ParquetULM(site,user,trajectory,low,norm,high,rating)
      
    })

  }

}