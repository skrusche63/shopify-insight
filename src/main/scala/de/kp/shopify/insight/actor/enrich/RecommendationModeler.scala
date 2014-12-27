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
 * RecommendationModeler is an actor that uses an association rule model, 
 * transforms the model into a user recommendation model and and registers 
 * the result in an ElasticSearch index.
 * 
 * This is part of the 'enrich' sub process that represents the third component 
 * of the data analytics pipeline.
 * 
 */
class RecommendationModeler(prepareContext:PrepareContext) extends BaseActor {

  override def receive = {
   
    case message:StartEnrich => {

      val req_params = message.data
      val uid = req_params(Names.REQ_UID)
      
      try {
        
        prepareContext.listener ! String.format("""[INFO][UID: %s] User recommendation model building started.""",uid)
        /*
         * STEP #1: Transform Shopify orders into last transaction itemsets; these
         * itemsets are then used as antecedents to filter those association rules
         * that match the antecedents
         */
        val itemsets = transform(prepareContext.getOrders(req_params))
         
        /*
         * STEP #2: Retrieve association rules from the Association Analysis engine
         */      
        val (service,request) = buildRemoteRequest(req_params)

        val response = prepareContext.getRemoteContext.send(service,request).mapTo[String]            
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {
                    
              prepareContext.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an engine error.""",uid)
 
              context.parent ! EnrichFailed(res.data)
              context.stop(self)

            } else {
              /*
               * STEP #3: Build product recommendations by merging the association
               * rules and the last transaction itemsets
               */            
              val recommendations = buildProductRecommendations(res,itemsets)
              /*
               * STEP #2: Create search index (if not already present);
               * the index is used to register the recommendations derived 
               * from the association rule model
               */
              val handler = new ElasticHandler()

              if (handler.putSources("orders","recommendations",recommendations) == false)
                throw new Exception("Indexing processing has been stopped due to an internal error.")

              prepareContext.listener ! String.format("""[INFO][UID: %s] User recommendation model building finished.""",uid)

              val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> "URECOM")            
              context.parent ! EnrichFinished(data)           
            
              context.stop(self)
             
            }
            
          }
          
        }
        /*
         * The Association Analysis engine returned an error message
         */
        response.onFailure {
          case throwable => {
                    
            prepareContext.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
            val params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ message.data
          
            context.parent ! EnrichFailed(params)           
            context.stop(self)
          
          }
	    
        }
         
      } catch {
        case e:Exception => {
                    
          prepareContext.listener ! String.format("""[ERROR][UID: %s] Retrieval of Association rules failed due to an internal error.""",uid)
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ message.data

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    }
    
  }

  private def buildRemoteRequest(params:Map[String,String]):(String,String) = {
    /*
     * Product recommendations are derived from the assocation rule model;
     * note, that we do not leverage the 'transaction' channel here.
     */
    val service = "association"
    val task = "get:rule"

    val data = new ASRHandler().get(params)
    val message = Serializer.serializeRequest(new ServiceRequest(service,task,data))
            
    (service,message)

  }
  
  private def buildProductRecommendations(response:ServiceResponse,itemsets:List[(String,String,List[Int])]):List[java.util.Map[String,Object]] = {
            
    val uid = response.data(Names.REQ_UID)
    val rules = Serializer.deserializeRules(response.data(Names.REQ_RESPONSE))

    val now = new java.util.Date()
    val timestamp = now.getTime()

    itemsets.map(itemset => {
      
      val (site,user,items) = itemset
      /*
       * Replace antecendent part of the retrieved rules by the itemset of the last 
       * user transaction, compute intersection ratio and restrict to those modified 
       * rules where antecedent and consequent are completely disjunct; finally sort
       * new rules by a) ratio AND b) confidence AND c) support and take best element 
       */
      val new_rules = rules.items.map(rule => {

        val intersect = items.intersect(rule.antecedent)
        val ratio = intersect.length.toDouble / items.length
                  
        (items,rule.consequent,rule.support,rule.total,rule.confidence,ratio)
      
      }).filter(r => (r._1.intersect(r._2).size == 0))
      
      val best_rule = new_rules.sortBy(x => (-x._6, -x._5, -x._3)).head
      val source = new java.util.HashMap[String,Object]()    

      source += Names.SITE_FIELD -> site
      source += Names.USER_FIELD -> user
      
      source += Names.UID_FIELD -> uid
      source += Names.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
        
      source += Names.CONSEQUENT_FIELD -> best_rule._2
        
      source += Names.SUPPORT_FIELD -> best_rule._3.asInstanceOf[Object]
      source += Names.TOTAL_FIELD -> best_rule._4.asInstanceOf[Object]
      
      source += Names.CONFIDENCE_FIELD -> best_rule._5.asInstanceOf[Object]        
      source += Names.WEIGHT_FIELD -> best_rule._6.asInstanceOf[Object]
        
      source

    })
 
  }
  private def transform(orders:List[Order]):List[(String,String,List[Int])] = {
    
    /*
     * Group orders by site & user
     */
    val result = orders.groupBy(o => (o.site,o.user)).map(o => {

      val (site,user) = o._1
      val (timestamp,itemset) = o._2.map(v => (v.timestamp,v.items)).toList.sortBy(_._1).last
      
      (site,user,itemset.map(_.item))
      
    })
    
    result.toList 
    
  }
  
}