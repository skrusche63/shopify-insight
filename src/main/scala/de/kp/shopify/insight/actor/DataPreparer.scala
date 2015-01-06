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

import akka.actor.Props
import de.kp.spark.core.Names

import de.kp.shopify.insight._
import de.kp.shopify.insight.model._

import de.kp.shopify.spark.ElasticRDD
import de.kp.shopify.insight.actor.prepare._

/**
 * The DataPreparer is a pipeline that combines multiple 
 * preparer actors in a single functional unti
 */
class DataPreparer(requestCtx:RequestContext) extends BaseActor {
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      val uid = req_params(Names.REQ_UID)
      
      try {
            
         val start = new java.util.Date().getTime.toString            
         requestCtx.listener ! String.format("""[INFO][UID: %s] Data preparation request received at %s.""",uid,start)

         /*
          * STEP #1: Retrieve orders from the database/orders index;
          * note, that we do not need to set es.query as we need all
          * orders registered till now
          */
         val esConfig = requestCtx.getESConfig
         esConfig.set(Names.ES_RESOURCE,("database/orders"))

         val elasticRDD = new ElasticRDD(requestCtx.sparkContext)
         
         val rawset = elasticRDD.read(esConfig)
         val orders = elasticRDD.orders(rawset)

         // TODO  Provide metadata description for shared parquet files.
         
         /*
          * STEP #2: Start ASRPreparer actor to create the ParquetASR table 
          * from the purchase history. It is shared with the Association 
          * Analysis engine. 
          * 
          * The transformed data are saved as a Parquet file.
          */
         val asr_preparer = context.actorOf(Props(new ASRPreparer(requestCtx,orders)))          
         asr_preparer ! StartPrepare(req_params)

         /*
          * STEP #3: Start STMPreparer actor to create the ParquetSTM table
          * from the purchase history. It is shared with the Intent Recognition 
          * engine. 
          * 
          * The transformed data are saved as a Parquet file.
          */
         val stm_preparer = context.actorOf(Props(new STMPreparer(requestCtx,orders)))          
         stm_preparer ! StartPrepare(req_params)
         
         /*
          * STEP #4: Start ITPPreparer actor to create the ParquetITP table
          * from the purchase history. This table specifies the user-specific 
          * item engagement and is the starting point of persona analysis
          * 
          * The transformed data are saved as a Parquet file.
          */
         val itp_preparer = context.actorOf(Props(new ITPPreparer(requestCtx,orders)))          
         itp_preparer ! StartPrepare(req_params)
         
         /*
          * STEP #5: Start FRPPreparer actor to create the ParquetFRP table 
          * from the purchase history. This table is used to specify the
          * temporal profile of a certain user.
          * 
          * The transformed data are saved as a Parquet file.
          */
         val frp_preparer = context.actorOf(Props(new FRPPreparer(requestCtx,orders)))          
         frp_preparer ! StartPrepare(req_params)

         /*
          * STEP #6: Start DOWPreparer actor to create the ParquetDOW table 
          * from the purchase history. This table is used to specify the
          * temporal profile of a certain user.
          * 
          * The transformed data are saved as a Parquet file.
          */
         val dow_preparer = context.actorOf(Props(new DOWPreparer(requestCtx,orders)))          
         dow_preparer ! StartPrepare(req_params)

         /*
          * STEP #7: Start HODPreparer actor to create the ParquetHOD table 
          * from the purchase history. This table is used to specify the
          * temporal profile of a certain user.
          * 
          * The transformed data are saved as a Parquet file.
          */
         val hod_preparer = context.actorOf(Props(new HODPreparer(requestCtx,orders)))          
         hod_preparer ! StartPrepare(req_params)

         /*
          * STEP #8: Start RFMPreparer actor to create the ParquetRFM table 
          * from the purchase history. This table is used to specify the
          * temporal profile of a certain user.
          * 
          * The transformed data are saved as a Parquet file.
          */
         val rfm_preparer = context.actorOf(Props(new RFMPreparer(requestCtx,orders)))          
         rfm_preparer ! StartPrepare(req_params)

         /*
          * STEP #9: Start SUMPreparer actor to update the orders/aggregates index 
          * from the purchase history. This index is used to provide aggregated or
          * summarized information.
          * 
          * The aggregated data are the starting point for comparing order perspectives
          * from different preparation timestamps.
          */
         val sum_preparer = context.actorOf(Props(new SUMPreparer(requestCtx,orders)))          
         sum_preparer ! StartPrepare(req_params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] Preparation exception: %s.""",uid,e.getMessage)
          context.parent ! PrepareFailed(req_params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
      
    }
  
    case message:PrepareFailed => {}
    
    case message:PrepareFinished => {}
  }

}