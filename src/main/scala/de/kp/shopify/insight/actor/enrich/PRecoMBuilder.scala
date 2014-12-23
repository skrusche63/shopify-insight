package de.kp.shopify.insight.actor.enrich
import de.kp.spark.core.Names
import de.kp.spark.core.model._
import de.kp.shopify.insight.PrepareContext
import de.kp.shopify.insight.model._
import de.kp.shopify.insight.io._
import de.kp.shopify.insight.elastic._
import de.kp.shopify.insight.source._
import scala.collection.JavaConversions._
import akka.actor.actorRef2Scala
import de.kp.shopify.insight.actor.BaseActor


/**
 * PRecoMBuilder is an actor that uses an association rule model, transforms
 * the model into a product recommendation model (PRM) and and registers the result 
 * in an ElasticSearch index.
 * 
 * This is part of the 'enrich' sub process that represents the third component of 
 * the data analytics pipeline.
 * 
 */
class PRecoMBuilder(prepareContext:PrepareContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {
      
      try {

        val params = message.data
        val uid = params(Names.REQ_UID)
        
        /*
         * STEP #1: Retrieve association rules from the Association 
         * Analysis engine
         */      
        val (service,request) = buildRemoteRequest(params)

        val response = prepareContext.getRemoteContext.send(service,request).mapTo[String]            
        response.onSuccess {
        
          case result => {
 
            val intermediate = Serializer.deserializeResponse(result)
            val recommendations = buildProductRecommendations(intermediate)
            /*
             * STEP #2: Create search index (if not already present);
             * the index is used to register the recommendations derived 
             * from the association rule model
             */
            val handler = new ElasticHandler()
            
            if (handler.createIndex(params,"orders","recommendations","recommendation") == false)
              throw new Exception("Indexing has been stopped due to an internal error.")
 
            prepareContext.listener ! String.format("""[UID: %s] Elasticsearch index created.""",uid)

            if (handler.putRules("orders","recommendations",recommendations) == false)
              throw new Exception("Indexing processing has been stopped due to an internal error.")

            prepareContext.listener ! String.format("""[UID: %s] Product recommendation perspective registered in Elasticsearch index.""",uid)

            val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "PRecoM")            
            context.parent ! EnrichFinished(data)           
            
            context.stop(self)
             
          }
          
        }
        /*
         * The Association Analysis engine returned an error message
         */
        response.onFailure {
          case throwable => {
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
         
      } catch {
        case e:Exception => {
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }

  private def buildRemoteRequest(params:Map[String,String]):(String,String) = {

    val service = "association"
    val task = "get:transaction"

    val data = new ASRHandler().get(params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  
  private def buildProductRecommendations(response:ServiceResponse):List[java.util.Map[String,Object]] = {
            
    val uid = response.data(Names.REQ_UID)
    /*
     * A recommendation request is dedicated to a certain 'site' and a list of users, 
     * and the result is a list of rules assigned to this input
     */
    val rules = Serializer.deserializeMultiUserRules(response.data(Names.REQ_RESPONSE))
    
    /*
     * Determine timestamp for the actual set of rules to be indexed
     */
    val now = new java.util.Date()
    val timestamp = now.getTime()
    
    rules.items.flatMap(entry => {

      /* 
       * Unique identifier to group all entries that 
       * refer to the same recommendation
       */      
      val rid = java.util.UUID.randomUUID().toString()
              
      val (site,user) = (entry.site,entry.user)
      entry.items.map(wrule => {
        /*
         * A weighted rule specify the intersection ration of the association rule
         * antecedent part with the users last transaction items
         */
        val source = new java.util.HashMap[String,Object]()    

        source += Names.SITE_FIELD -> site
        source += Names.USER_FIELD -> user
      
        source += Names.UID_FIELD -> uid
        source += Names.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
      
        source += Names.RECOMMENDATION_FIELD -> rid
        
        source += Names.CONSEQUENT_FIELD -> wrule.consequent
        
        source += Names.SUPPORT_FIELD -> wrule.support.asInstanceOf[Object]
        source += Names.CONFIDENCE_FIELD -> wrule.confidence.asInstanceOf[Object]
        
        source += Names.WEIGHT_FIELD -> wrule.weight.asInstanceOf[Object]
        
        source
        
      })
          
    })

  }
  
}