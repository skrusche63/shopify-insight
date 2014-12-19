package de.kp.shopify.insight.actor

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import akka.actor.ActorRef

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.io._
import de.kp.spark.core.elastic._

import de.kp.spark.core.spec.FieldBuilder

import de.kp.shopify.insight.io.QueryBuilder

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.source._

/**
 * ElasticPreparer is an actor that transforms a certain 'amount' database
 * into a 'state' database and prepares intent recognition taks such as
 * 'loyalty' and 'purchase' requests
 */
class ElasticPreparer(@transient sc:SparkContext,listener:ActorRef) extends BaseActor {

  override def receive = {
    
    case req:ServiceRequest => {
      
      val uid = req.data(Names.REQ_UID)

      try {
        /*
         * ElasticPreparer is restricted to Elasticsearch as sink
         */
        require(req.data(Names.REQ_SINK) == Sinks.ELASTIC)
        
        val Array(task,topic) = req.task.split(":")
        topic match {
          
          case "amount" => {
            /*
             * STEP#1: Retrieve amount database from Elasticsearch
             * and convert data into state database; the request
             * parameters required are:
             * 
             * - site (String)
             * - uid (String)
             * - name (String)
             * 
             * - source (String)
             * 
             * The following parameters are internal and are added
             * 
             * - source.index
             * - source.type
             * 
             * - query
             */
            val data = Map(
              /*
               * The subsequent parameters are copied from
               * the external request parameters
               */
              Names.REQ_UID -> req.data(Names.REQ_UID),
              
              Names.REQ_SITE -> req.data(Names.REQ_SITE),
              Names.REQ_NAME -> req.data(Names.REQ_NAME),
              /*
               * The subsequent parameters are added as
               * these are unknown to the user
               */
              Names.REQ_SOURCE_INDEX -> "orders",
              Names.REQ_SOURCE_TYPE  -> "amount",
              
              Names.REQ_QUERY -> QueryBuilder.get(Sources.ELASTIC,"amount")
              
            )

            val source = new AmountSource(sc)            
            val dataset = source.get(new ServiceRequest("","",data))

            listener ! String.format("""[UID: %s] Amount data retrieved from Elasticsearch and transformed into states.""",uid)
            
            /*
             * STEP#2: Create index to register states
             */
            if (createIndex(req,"orders","states") == false)
              throw new Exception("Feed processing has been stopped due to an internal error.")
            
            /*
             * STEP#3: Register states
             */
            dataset.collect().foreach(record => {
              
              val (site,user,timestamp,state) = record
              val data = Map(
                
                Names.SITE_FIELD -> site,
                Names.USER_FIELD -> user,
                
                Names.TIMESTAMP_FIELD -> timestamp.toString,
                Names.STATE_FIELD -> state
                
              )
             
              if (putState(new ServiceRequest("","",data),"orders","states"))
                  throw new Exception("Preparation process has been stopped due to an internal error.")
              
            })
            
            listener ! String.format("""[UID: %s] Preparation request finished.""",uid)
 
          }
          
          case _ => {/* do nothing */}
        }

      } catch {
        case e:Exception => listener ! String.format("""[UID: %s] Preparation request exception: %s.""",uid,e.getMessage)

      } finally {
        
        context.stop(self)
        
      }
      
    }
    
  }
  
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
      val data = Map(Names.REQ_NAME -> mapping) ++  req.data 
     
      if (fields.isEmpty == false) cache.addFields(new ServiceRequest("","",data), fields.toList)
     
      true
    
    } catch {
      case e:Exception => false
    }
    
  }
  
  private def putState(req:ServiceRequest,index:String,mapping:String):Boolean = {
     
    try {
    
      val writer = new ElasticWriter()
        
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        val source = new ElasticStateBuilder().createSource(req.data)
        /*
         * Writing this source to the respective index throws an
         * exception in case of an error; note, that the writer is
         * automatically closed 
         */
        writer.write(index, mapping, source)
      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }
  
}