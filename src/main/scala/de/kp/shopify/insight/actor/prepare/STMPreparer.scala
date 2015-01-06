package de.kp.shopify.insight.actor.prepare
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import de.kp.spark.core.Names
import de.kp.shopify.insight._
import de.kp.shopify.insight.model._
import de.kp.shopify.insight.analytics.StateHandler
import akka.actor.actorRef2Scala
import de.kp.shopify.insight.actor.BaseActor
import scala.reflect.runtime.universe

/**
 * The STMPreparer generates a state representation for all customers
 * that have purchased at least twice since the start of the collection
 * of the Shopify orders.
 * 
 * Note, that we actually do not distinguish between customers that have 
 * a more frequent purchase behavior, and those, that have purchased only
 * twice.
 * 
 */
class STMPreparer(requestCtx:RequestContext,orders:RDD[InsightOrder]) extends BaseActor {
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = msg.data
      val uid = req_params(Names.REQ_UID)
      
      try {

        val sc = requestCtx.sparkContext
        val table = orders.groupBy(x => (x.site,x.user)).filter(_._2.size > 1).flatMap(p => {

          val (site,user) = p._1  
      
          /* Compute time ordered list of (amount,timestamp) */
          val user_orders = p._2.map(x => (x.amount,x.timestamp)).toList.sortBy(_._2)      

          val user_amounts = user_orders.map(_._1)       
          /*
           * Compute the amount difference and respective sub state from 
           * the subsequent pairs of user amounts; the amount handler holds 
           * a predefined configuration to map a pair onto a state
           */
          val user_amount_states = user_amounts.zip(user_amounts.tail).map(x => StateHandler.stateByAmount(x._2,x._1))

          val user_timestamps = user_orders.map(_._2)
          /*
           * Compute the timespan and the respective sub state from 
           * the subsequent pairs of user timestamps; the state handler 
           * holds a predefined configuration to map a pair onto a state
           */
          val user_time_states = user_timestamps.zip(user_timestamps.tail).map(x => StateHandler.stateByTime(x._2,x._1))

          /*
           * Build user states and zip result with user_timestamps; a user state encloses
           * the amount & time difference and the respective genuine state description
           */
          val user_states = user_amount_states.zip(user_time_states).map(x => x._1 + x._2)
          
          val user_records = user_amounts.tail.zip(user_timestamps.tail).zip(user_states)          
          user_records.map(x => {
            
            val (amount,timestamp) = x._1
            val state = x._2
            
            ParquetSTM(site,user,amount,timestamp,state)
            
          
          })
        })
        
        val sqlCtx = new SQLContext(sc)
        import sqlCtx.createSchemaRDD

        /* 
         * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
         * allowing it to be stored using Parquet. 
         */
        val store = String.format("""%s/STM/%s""",requestCtx.getBase,uid)         
        table.saveAsParquetFile(store)

        val params = Map(Names.REQ_MODEL -> "STM") ++ req_params
        context.parent ! PrepareFinished(params)

      } catch {
        case e:Exception => {
          /* 
           * In case of an error the message listener gets informed, and also
           * the data processing pipeline in order to stop further sub processes 
           */
          requestCtx.listener ! String.format("""[ERROR][UID: %s] STM preparation exception: %s.""",uid,e.getMessage)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PrepareFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    }
  
  }
}