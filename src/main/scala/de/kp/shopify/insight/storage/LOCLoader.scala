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
 * LOCLoader class directly loads the results of the LOCPreparer
 * into the customers/locations index.
 */
class LOCLoader(ctx:RequestContext,params:Map[String,String]) extends BaseLoader(ctx,params) {

  override def load(params:Map[String,String]) {

    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    val parquetFile = extract(store)

    ctx.listener ! String.format("""[INFO][UID: %s] Parquet file successfully retrieved.""",uid)
        
    val sources = transform(params,parquetFile)

    if (ctx.putSources("customers","locations",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
    
  }

  private def extract(store:String):RDD[ParquetLOC] = {
    
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
      
      val ip_address = data("ip_address").asInstanceOf[String]
      val timestamp = data("timestamp").asInstanceOf[Long]

      val countryname = data("countryname").asInstanceOf[String]
      val countrycode = data("countrycode").asInstanceOf[String]

      val region = data("region").asInstanceOf[String]
      val regionname = data("regionname").asInstanceOf[String]

      val areacode = data("areacode").asInstanceOf[Int]
      val dmacode = data("dmacode").asInstanceOf[Int]

      val metrocode = data("metrocode").asInstanceOf[Int]
      val city = data("city").asInstanceOf[String]
 
      val postalcode = data("postalcode").asInstanceOf[String]

      val lat = data("lat").asInstanceOf[Double]
      val lon = data("lon").asInstanceOf[Double]

      ParquetLOC(
          site,
          user,
  
          ip_address,
          timestamp,
    
          countryname,
          countrycode,

          region,
          regionname,
  
          areacode,
          dmacode,
  
          metrocode,
          city,
  
          postalcode,
	  
          lat,
          lon
      )
      
    })

  }
  
  private def transform(params:Map[String,String],dataset:RDD[ParquetLOC]):List[XContentBuilder] = {
            
    dataset.map(x => {
      
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /********** METADATA **********/
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))
      
      /* timestamp */
      builder.field(Names.TIMESTAMP_FIELD,params("timestamp").toLong)

	  /* site */
	  builder.field("site",x.site)
      
      /********** LOCATION DATA **********/

	  /* user */
	  builder.field("user",x.user)

	  /* ip_address */
	  builder.field("ip_address",x.ip_address)

	  /* time */
	  builder.field("time",x.timestamp)

	  /* countryname */
	  builder.field("countryname",x.countryname)

	  /* countrycode */
	  builder.field("countrycode",x.countrycode)

	  /* region */
	  builder.field("region",x.region)

	  /* regionname */
	  builder.field("regionname",x.regionname)

	  /* areacode */
	  builder.field("areacode",x.areacode)

	  /* dmacode */
	  builder.field("dmacode",x.dmacode)

	  /* metrocode */
	  builder.field("metrocode",x.metrocode)

	  /* city */
	  builder.field("city",x.city)

	  /* postalcode */
	  builder.field("postalcode",x.postalcode)

	  /* location */
	  builder.startObject("location")
	  
	  /* lat */
	  builder.field("lat",x.lat)

	  /* lon */
	  builder.field("lon",x.lon)
	  
	  builder.endObject()
	  
	  builder.endObject()
      builder
      
    }).collect.toList

  }
  
}