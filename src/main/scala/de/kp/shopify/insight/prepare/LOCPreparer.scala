package de.kp.shopify.insight.prepare
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
import org.apache.spark.rdd.RDD
import de.kp.spark.core.Names
import de.kp.shopify.insight._
import de.kp.shopify.insight.model._
import de.kp.shopify.insight.actor.BaseActor
import de.kp.shopify.insight.geoip.LocationFinder
import akka.actor.actorRef2Scala
import scala.reflect.runtime.universe
/**
 * The LOCPreparer evaluates the IP addresses and timestamps of 
 * customer purchase transactions for a certain period of time
 * and maps the IP address onto a geospatial location using the
 * GeoLiteCity database. These data can be used to build movement
 * profiles for the customers.
 * 
 * The LOCPreparer does NOT take specific (RFM) customer types 
 * into account, as we cannot see any necessity to do so.
 */
class LOCPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor(ctx) {
  
  import sqlc.createSchemaRDD
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data      
      val uid = req_params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      ctx.listener ! String.format("""[INFO][UID: %s] LOC preparation request received at %s.""",uid,start)
      
      try {
        
        val table = orders.groupBy(x => (x.site,x.user)).flatMap(x => {
          
          val (site,user) = x._1
          /* Determine timestamp of order and associated IP address */
          val data = x._2.map(v => (v.ip_address,v.timestamp)).toSeq.sortBy(v => v._2)
          data.map(v => {
            
            val (ip_address,timestamp) = v
            val loc = LocationFinder.locate(ip_address)
           
            ParquetLOC(
              site,
              user,
              
              ip_address,
              timestamp,
              
              loc.countryname,
              loc.countrycode,
            
              loc.region,
              loc.regionname,
            
              loc.areacode,
              loc.dmacode,
            
              loc.metrocode,
              loc.city,
            
              loc.postalcode,
            
              loc.lat,
              loc.lon
            )
          })
          
        })
        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/LOC/%s""",ctx.getBase,uid)         
        table.saveAsParquetFile(store)

        ctx.listener ! String.format("""[INFO][UID: %s] LOC preparation finished.""",uid)

        val params = Map(Names.REQ_MODEL -> "LOC") ++ req_params
        context.parent ! PrepareFinished(params)
        
      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          ctx.listener ! String.format("""[ERROR][UID: %s] LOC preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
  
}