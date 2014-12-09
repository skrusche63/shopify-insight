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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.io._
import de.kp.spark.core.elastic._

import de.kp.spark.core.spec.FieldBuilder

import de.kp.shopify.insight.{RemoteContext,ShopifyContext}
import de.kp.shopify.insight.io.RequestBuilder

import de.kp.shopify.insight.model._

class FeedWorker(ctx:RemoteContext) extends WorkerActor(ctx) {
  
  private val stx = new ShopifyContext()  
  override def receive = {
    
    case req:ServiceRequest => {

      val origin = sender
      val service = req.service
      
      try {
        
        val Array(task,topic) = req.task.split(":")
        topic match {
          /*
           * Retrieve orders from Shopify store via REST interface, prepare index
           * for 'items' and (after that) track each order as 'item'
           */
          case "order" => {
            /*
             * STEP #1: Retrieve orders from a certain shopify store
             */
            val orders = stx.getOrders(req)
            /*
             * STEP #2: Inform requestor about receipt of tracking data
             */
            val uid = req.data(Names.REQ_UID)      
            val res_data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.TRACKING_DATA_RECEIVED(uid))
      
            origin ! Serializer.serializeResponse(new ServiceResponse(req.service,req.task,res_data,ResponseStatus.SUCCESS))
            /*
             * STEP #3: Create search indexes (if not already present)
             */
            var success = false
            /*
             * The 'amount' index (mapping) is used by Intent Recognition and
             * supports the Recency-Frequency-Monetary (RFM) model
             */
            success = createIndex(req,"orders","amount")
            if (success == false) throw new Exception("Feed processing has been stopped due to an internal error.")
            /*
             * The 'item' index (mapping) specifies a transaction database and
             * is used by Association Analysis, Series Analysis and the Recommender
             */
            success = createIndex(req,"orders","item")
            if (success == false) throw new Exception("Feed processing has been stopped due to an internal error.")

            /*
             * STEP #4: Build tracking requests to send the collected orders to
             * the respective service or engine; the orders are sent independently 
             * following a fire-and-forget strategy
             */
            val builder = new RequestBuilder()
            for (order <- orders) {
              /*
               * The 'amount' perspective of the order is built and tracked
               */
              success = trackOrder(builder.build(order, "amount"),"orders","amount")
              if (success == false) throw new Exception("Feed processing has been stopped due to an internal error.")
              /*
               * The 'item' perspective of the order is built and tracked
               */
              success = trackOrder(builder.build(order, "item"),"orders","item")
              if (success == false) throw new Exception("Feed processing has been stopped due to an internal error.")
                
            }

          }
          
          case "product" => {
            
            /* actually not supported */
            
          }
            
          case _ => {/* do nothing */}
          
        }
        
      } catch {
        case e:Exception => origin ! failure(req,e.getMessage)

      }
      
    }
    
  }
  
  override def getResponse(service:String,message:String) = null 
  
  private def createIndex(req:ServiceRequest,index:String,mapping:String):Boolean = {
    
    try {
        
      val builder = ElasticBuilderFactory.getBuilder(mapping,mapping,List.empty[String],List.empty[String])
      val indexer = new ElasticIndexer()
    
      indexer.create(index,mapping,builder)
      indexer.close()
      
      /*
       * Raw data that are ingested by the tracking functionality do not have
       * to be specified by a field or metadata specification; we therefore
       * and the field specification here as an internal feature
       */        
      val fields = new FieldBuilder().build(req,mapping)
      
      /*
       * The name of the model to which these fields refer cannot be provided
       * by the user; we therefore have to re-pack the service request to set
       * the name of the model
       */
      val excludes = List(Names.REQ_NAME)
      val data = Map(Names.REQ_NAME -> mapping) ++  req.data.filter(kv => excludes.contains(kv._1) == false)  
     
      if (fields.isEmpty == false) cache.addFields(new ServiceRequest("","",data), fields.toList)
     
      true
    
    } catch {
      case e:Exception => false
    }
    
  }
  
  private def trackOrder(req:ServiceRequest,index:String,mapping:String):Boolean = {
     
    try {
    
      val writer = new ElasticWriter()
        
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
       throw new Exception(msg)
      
      } else {
     
        mapping match {
          
          case "amount" => {
      
            val source = new ElasticAmountBuilder().createSource(req.data)
            /*
             * Writing this source to the respective index throws an
             * exception in case of an error; note, that the writer is
             * automatically closed 
             */
            writer.write(index, mapping, source)
            
          }
          case "item" => {
      
            /*
             * Data preparation comprises the extraction of all common 
             * fields, i.e. timestamp, site, user and group. The 'item' 
             * field may specify a list of purchase items and has to be 
             * processed differently.
             */
            val source = new ElasticItemBuilder().createSource(req.data)
            /*
             * The 'item' field specifies a comma-separated list
             * of item (e.g.) product identifiers. Note, that every
             * item is actually indexed individually. This is due to
             * synergy effects with other data sources
             */
            val items = req.data(Names.ITEM_FIELD).split(",")
            /*
             * A trackable event may have a 'score' field assigned;
             * note, that this field is optional
             */
            val scores = if (req.data.contains(Names.REQ_SCORE)) req.data(Names.REQ_SCORE).split(",").map(_.toDouble) else Array.fill[Double](items.length)(0)

            val zipped = items.zip(scores)
            for  ((item,score) <- zipped) {
              /*
               * Set or overwrite the 'item' field in the respective source
               */
              source.put(Names.ITEM_FIELD, item)
              /*
               * Set or overwrite the 'score' field in the respective source
               */
              source.put(Names.SCORE_FIELD, score.asInstanceOf[Object])
              /*
               * Writing this source to the respective index throws an
               * exception in case of an error; note, that the writer is
               * automatically closed 
               */
              writer.write(index, mapping, source)
            }
            
          }
          case _ => {/* cannot happen */}
          
        }
      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }
  
}