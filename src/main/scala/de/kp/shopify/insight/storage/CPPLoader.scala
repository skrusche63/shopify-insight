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

import de.kp.shopify.insight.actor._
import de.kp.shopify.insight.model._

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

/**
 * CPPLoader class loads the results of the CPPProfiler
 * into the customers/personas index.
 */
class CPPLoader(ctx:RequestContext,params:Map[String,String]) extends BaseLoader(ctx,params) {
  
  override def load(params:Map[String,String]) {

    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    val parquetFile = extract(store)

    ctx.listener ! String.format("""[INFO][UID: %s] Parquet file successfully retrieved.""",uid)
        
    val sources = transform(params,parquetFile)

    if (ctx.putSources("customers","profiles",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
    
  }

  private def extract(store:String):RDD[ParquetCPP] = {
    
    /* 
     * Read in the parquet file created above. Parquet files are self-describing 
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

      val recency = data("recency").asInstanceOf[Int]

      val frequency = data("frequency").asInstanceOf[Int]      
      val monetary = data("monetary").asInstanceOf[Double]

      val rval = data("rval").asInstanceOf[Int]
      val fval = data("fval").asInstanceOf[Int]
      
      val mval = data("mval").asInstanceOf[Int]
      val lval = data("lval").asInstanceOf[Int]

      val rfm_type = data("rfm_type").asInstanceOf[Int]
      
      val d_type = data("d_type").asInstanceOf[Int]
      val d_distance = data("d_distance").asInstanceOf[Double]
      
      val h_type = data("h_type").asInstanceOf[Int]
      val h_distance = data("h_distance").asInstanceOf[Double]
      
      val r_type = data("r_type").asInstanceOf[Int]
      val r_distance = data("r_distance").asInstanceOf[Double]
      
      val p_type = data("p_type").asInstanceOf[Int]
      val p_distance = data("p_distance").asInstanceOf[Double]

      ParquetCPP(
          site,
          user,
          recency,
          frequency,
          monetary,
          rval,
          fval,
          mval,
          lval,
          rfm_type,
          d_type,
          d_distance,
          h_type,
          h_distance,
          r_type,
          r_distance,
          p_type,
          p_distance
      )
      
    })
    
  }
  
  private def transform(params:Map[String,String],dataset:RDD[ParquetCPP]):List[XContentBuilder] = {
            
    dataset.map(x => {
      
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /********** METADATA **********/
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))
      
      /* timestamp */
      builder.field(Names.TIMESTAMP_FIELD,params("timestamp").toLong)
	  
	  /* site */
      builder.field(Names.SITE_FIELD,x.site)
      
      /********** PROFILE DATA **********/
	  
	  /* user */
      builder.field(Names.USER_FIELD,x.user)

	  /* recency */
	  builder.field("recency",x.recency)

	  /* frequency */
	  builder.field("frequency",x.frequency)

	  /* amount */
	  builder.field("amount",x.monetary)

	  /* r_segment */
	  builder.field("r_segment",x.rval)

	  /* f_segment */
	  builder.field("f_segment",x.fval)

	  /* m_segment */
	  builder.field("m_segment",x.mval)

	  /* l_segment */
	  builder.field("l_segment",x.lval)

	  /* customer_type */
	  builder.field("customer_type",x.rfm_type)

	  /* d_type */
	  builder.field("d_type",x.d_type)

	  /* d_distance */
	  builder.field("d_distance",x.d_distance)

	  /* h_type */
	  builder.field("h_type",x.h_type)

	  /* h_distance */
	  builder.field("h_distance",x.h_distance)

	  /* r_type */
	  builder.field("r_type",x.r_type)

	  /* r_distance */
	  builder.field("r_distance",x.r_distance)

	  /* p_type */
	  builder.field("p_type",x.p_type)

	  /* p_distance */
	  builder.field("p_distance",x.p_distance)
	  
	  builder.endObject()
	  
	  builder.endObject()
      builder
      
    }).collect.toList
  }

}