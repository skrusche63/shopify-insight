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
 import org.elasticsearch.index.query._
 import org.elasticsearch.common.xcontent.XContentFactory
 import scala.collection.mutable.Buffer
 import de.kp.shopify.insight.prepare.LOCPreparer
 import de.kp.shopify.insight.prepare.RFMPreparer

/**
 * The DataPreparer is a pipeline that combines multiple 
 * preparer actors in a single functional unti
 */
class DataPreparer(ctx:RequestContext) extends BaseActor(ctx) {
  
  private val STEPS = Buffer.empty[String]
  private val STEPS_COMPLETE = 9
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      val uid = req_params(Names.REQ_UID)
      
      try {
            
        val start = new java.util.Date().getTime.toString            
        ctx.listener ! String.format("""[INFO][UID: %s] Data preparation request received at %s.""",uid,start)

        /**********************************************************************
         *      
         *                       SUB PROCESS 'PREPARE'
         * 
         *********************************************************************/
      
        createESIndex(req_params)
        /*
         * Register this data preparation task in the respective
         * 'database/tasks' index
         */
        registerESTask(req_params)

        /*
         * STEP #1: Retrieve orders from the database/orders index;
         * note, that we do not need to set es.query as we need all
         * orders registered till now
         */
        val esConfig = ctx.getESConfig
        esConfig.set(Names.ES_RESOURCE,("database/orders"))

        if (req_params.contains(Names.REQ_QUERY))
          esConfig.set(Names.ES_QUERY,req_params(Names.REQ_QUERY))
      
        else 
          esConfig.set(Names.ES_QUERY,query(req_params))
      
        val elasticRDD = new ElasticRDD(ctx.sparkContext)
         
        val rawset = elasticRDD.read(esConfig)
        val orders = elasticRDD.orders(rawset)

        /*
         * STEP #2: Start RFMPreparer actor to create the ParquetRFM table 
         * from the purchase history. RFM Analysis is the starting point
         * for all other preparation steps as this analysis segments the
         * customer base under consideration to 8 different customer types.
         */
        val rfm_preparer = context.actorOf(Props(new RFMPreparer(ctx,orders)))          
        rfm_preparer ! StartPrepare(req_params)
         
        /*
         * STEP #3: Start ASRPreparer actor to create the ParquetASR table 
         * from the purchase history. It is shared with the Association 
         * Analysis engine. 
         */
        //val asr_preparer = context.actorOf(Props(new ASRPreparer(requestCtx,orders)))          
        //asr_preparer ! StartPrepare(req_params)

        /*
         * STEP #4: Start STMPreparer actor to create the ParquetSTM table
         * from the purchase history. It is shared with the Intent Recognition 
         * engine. 
         */
        //val stm_preparer = context.actorOf(Props(new STMPreparer(requestCtx,orders)))          
        //stm_preparer ! StartPrepare(req_params)
         
        /*
         * STEP #5: Start ITPPreparer actor to create the ParquetITP table
         * from the purchase history. This table specifies the user-specific 
         * item engagement and is the starting point of persona analysis
         */
        //val itp_preparer = context.actorOf(Props(new ITPPreparer(requestCtx,orders)))          
        //itp_preparer ! StartPrepare(req_params)
         
        /*
         * STEP #6: Start FRPPreparer actor to create the ParquetFRP table 
         * from the purchase history. This table is used to specify the
         * temporal profile of a certain user.
         */
        //val frp_preparer = context.actorOf(Props(new FRPPreparer(requestCtx,orders)))          
        //frp_preparer ! StartPrepare(req_params)

        /*
         * STEP #7: Start DOWPreparer actor to create the ParquetDOW table 
         * from the purchase history. This table is used to specify the
         * temporal profile of a certain user.
         * 
         * The transformed data are saved as a Parquet file.
         */
        //val dow_preparer = context.actorOf(Props(new DOWPreparer(requestCtx,orders)))          
        //dow_preparer ! StartPrepare(req_params)

        /*
         * STEP #8: Start HODPreparer actor to create the ParquetHOD table 
         * from the purchase history. This table is used to specify the
         * temporal profile of a certain user.
         * 
         * The transformed data are saved as a Parquet file.
         */
        //val hod_preparer = context.actorOf(Props(new HODPreparer(requestCtx,orders)))          
        //hod_preparer ! StartPrepare(req_params)

        /*
         * STEP #9: Start LOCPreparer actor to create the ParquetRFM table 
         * from the purchase history. This table is used to specify the
         * geospatial profile of a certain user.
         */
        val loc_preparer = context.actorOf(Props(new LOCPreparer(ctx,orders)))          
        loc_preparer ! StartPrepare(req_params)
        
        /*
         * STEP #10: Start CHNPreparer actor to create the ParquetCHN table 
         * from the purchase history. This table is used to specify the
         * churn profile of a certain user.
         */
        //val chn_preparer = context.actorOf(Props(new CHNPreparer(requestCtx,orders)))          
        //chn_preparer ! StartPrepare(req_params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          ctx.listener ! String.format("""[ERROR][UID: %s] Preparation exception: %s.""",uid,e.getMessage)
          context.parent ! PrepareFailed(req_params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
      
    }
  
    case message:PrepareFailed => {
      /*
       * The Prepare actors already sent an error message to the message listener; 
       * what is left here is to forward the failed message to the data pipeline (parent)
       */
      context.parent ! PrepareFailed(message.data)
      context.stop(self)
      
    }
    
    case message:PrepareFinished => {
      /*
       * This message is sent by one of the Preparer actors and indicates that the
       * data preparation of a certain dimension has been successfully finished.
       */  
      val model = message.data(Names.REQ_MODEL)
      if (List("ASR","CHN","DOW","FRP","HOD","ITP","LOC","RFM","STM").contains(model)) STEPS += model
      
      if (STEPS.size == STEPS_COMPLETE) {

        val params = message.data.filter(kv => kv._1 == Names.REQ_MODEL)
        context.parent ! PrepareFinished(params)
      
        context.stop(self)
        
      } else {
        /*
         * do nothing 
         */
      }
      
    }
  }
  
  /**
   * This method registers the data preparation task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerESTask(params:Map[String,String]) = {
    
    val uid = params(Names.REQ_UID)
    val key = "prepare:" + uid
    
    val task = "data preparation"
    /*
     * Note, that we do not specify additional
     * payload data here
     */
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* key */
	builder.field("key",key)
	
	/* task */
	builder.field("task",task)
	
	/* timestamp */
	builder.field("timestamp",params("timestamp").toLong)

    /* created_at_min */
	builder.field("created_at_min",unformatted(params("created_at_min")))
	
    /* created_at_max */
	builder.field("created_at_max",unformatted(params("created_at_max")))
	
	builder.endObject()
	/*
	 * Register data in the 'database/tasks' index
	 */
	ctx.putSource("database","tasks",builder)

  }

  private def createESIndex(params:Map[String,String]) {
    
    val uid = params(Names.REQ_UID)
    /*
     * Create search index (if not already present)
     * 
     * The 'tasks' index (mapping) specified an administrative database
     * where all steps of a certain synchronization or data analytics
     * task are registered
     */
    
    if (ctx.createIndex(params,"database","tasks","task") == false)
      throw new Exception("Index creation for 'database/tasks' has been stopped due to an internal error.")
   
    ctx.listener ! String.format("""[INFO][UID: %s] Elasticsearch database/tasks index created.""",uid)
    
  }
  
  private def query(params:Map[String,String]):String = {
    
    val created_at_min = unformatted(params("created_at_min"))
    val created_at_max = unformatted(params("created_at_max"))
            
    val filters = Buffer.empty[FilterBuilder]
    filters += FilterBuilders.rangeFilter("time").from(created_at_min).to(created_at_max)
    
    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)
    
    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    qbuilder.toString
    
  }

}