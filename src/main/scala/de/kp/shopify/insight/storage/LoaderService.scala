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

import akka.actor._

import org.apache.spark.rdd.RDD

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import org.clapper.argot._

import de.kp.spark.core.Names
import de.kp.spark.core.SparkService

import de.kp.shopify.insight.{Configuration,RequestContext}
import de.kp.shopify.insight.actor.MessageListener

import de.kp.shopify.insight.model._

import org.elasticsearch.common.xcontent.XContentFactory
import scala.collection.mutable.{Buffer,HashMap}

class LoaderService(val appName:String) extends SparkService {
  
  protected val sc = createCtxLocal("LoaderContext",Configuration.spark)      
  protected val system = ActorSystem("LoaderSystem")

  protected val inbox = Inbox.create(system)
  
  sys.addShutdownHook({
    /*
     * In case of a system shutdown, we also make clear
     * that the SparkContext is properly stopped as well
     * as the respective Akka actor system
     */
    sc.stop
    system.shutdown
    
  })
  
  /*
   * The listener actor is an overall listener that retrieves the error and
   * interim messages from all the other actors
   */
  protected val listener = system.actorOf(Props(new MessageListener()))
  protected val ctx = new RequestContext(sc,listener)
  
  protected def createParams(args:Array[String]):Map[String,String] = {

    import ArgotConverters._
     
    val parser = new ArgotParser(
      programName = appName,
      compactUsage = true,
      preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
    )
    
    /*
     * The 'uid' parameter must be provided with the -uid option
     */
    val uid = parser.option[String](List("uid"),"uid","Unique preparation identifier")
    val job = parser.option[String](List("job"),"job","Unique job descriptor")

    val customer = parser.option[Int](List("customer"),"customer","Customer type.")

    parser.parse(args)
      
    /* Validate parameters */
    if (uid.hasValue == false)
      throw new Exception("Parameter 'uid' is missing.")
    
    if (job.hasValue == false)
      throw new Exception("Parameter 'job' is missing.")
  
    val jobs = List("CLS","CPF","LOC","POM","PPF","PRM","RFM")
    if (jobs.contains(job.value.get) == false)
      throw new Exception("Job parameter must be one of [CLS, CPF, LOC, POM, PPF, PRM, RFM].")
     
    /* Collect parameters */
    val params = HashMap.empty[String,String]
     
    params += "uid" -> uid.value.get
    params += "job" -> job.value.get
    
    params += "customer" -> customer.value.getOrElse(0).toString
    params += "timestamp" -> new DateTime().getMillis().toString
    
    params.toMap
    
  }
  
  protected def initialize(params:Map[String,String]) {
    /*
     * Create Elasticsearch task database and register 
     * the respective task in the database
     */
    createESIndexes(params)
    registerESTask(params)
    
  }
  /**
   * This method registers the data storage task in the respective
   * Elasticsearch index; this information supports administrative
   * tasks such as the monitoring of this insight server
   */
  private def registerESTask(params:Map[String,String]) = {
    
    val key = "LOAD:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data storage with " + appName + "."
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
	
	builder.endObject()
	/*
	 * Register data in the 'database/tasks' index
	 */
	ctx.putSource("database","tasks",builder)

  }

  private def createESIndexes(params:Map[String,String]) {
    
    val uid = params(Names.REQ_UID)
    /*
     * Create search indexes (if not already present)
     * 
     * The 'task' index (mapping) specified an administrative database
     * where all steps of a certain synchronization or data analytics
     * task are registered
     * 
     * The 'forecast' index (mapping) specifies a sales forecast database
     * derived from the Markovian rules built by the Intent Recognition
     * engine
     * 
     * The 'location' index (mapping) specifies a user movement database
     * derived from IP addresses and timestamps provided by user orders
     * 
     * The 'loyalty' index (mapping) specifies a user loyalty database
     * that describes customers with their assigned loyalty segment
     * 
     * The 'metric' index (mapping) specifies a statistics database 
     * that holds synchronized aggregated order data relevant for the 
     * insight server
     * 
     * The 'recommendation' index (mapping) specifies a product recommendation
     * database derived from the Association rules and the last items purchased
     * 
     * The 'rule' index (mapping) specifies a product association rule 
     * database computed by the Association Analysis engine
     * 
     * The 'segment' index (mapping) specifies the customer segmentation 
     * database due to the applied RFM segmentation, and also the product
     * segmentation base due to the applied PPF segmentation
     */
    
    if (ctx.createIndex(params,"database","tasks","task") == false)
      throw new Exception("Index creation for 'database/tasks' has been stopped due to an internal error.")
 
    /********** ORDER **********/
    
    if (ctx.createIndex(params,"orders","metrics","POM") == false)
      throw new Exception("Index creation for 'orders/metrics' has been stopped due to an internal error.")
 
    /********** PRODUCT ********/
            
    if (ctx.createIndex(params,"products","rules","PRM") == false)
      throw new Exception("Index creation for 'products/rules' has been stopped due to an internal error.")

    if (ctx.createIndex(params,"products","segments","PPF") == false)
      throw new Exception("Index creation for 'products/segments' has been stopped due to an internal error.")
 
    /********** CUSTOMERS ******/
            
    if (ctx.createIndex(params,"customers","loyalties","CLS") == false)
      throw new Exception("Index creation for 'users/loyalties' has been stopped due to an internal error.")

    if (ctx.createIndex(params,"customers","locations","LOC") == false)
      throw new Exception("Index creation for 'users/locations' has been stopped due to an internal error.")
            
    if (ctx.createIndex(params,"customers","segments","RFM") == false)
      throw new Exception("Index creation for 'users/segments' has been stopped due to an internal error.")
    
    if (ctx.createIndex(params,"customers","forecasts","CPF") == false)
      throw new Exception("Index creation for 'users/forecasts' has been stopped due to an internal error.")

    // TODO
            
    if (ctx.createIndex(params,"users","recommendations","recommendation") == false)
      throw new Exception("Index creation for 'users/recommendations' has been stopped due to an internal error.")
  
    ctx.listener ! String.format("""[INFO][UID: %s] Elasticsearch indexes created.""",uid)
     
  }
  
  private def unformatted(date:String):Long = {

    //2008-12-31 03:00
    val pattern = "yyyy-MM-dd HH:mm"
    val formatter = DateTimeFormat.forPattern(pattern)
 
    formatter.parseMillis(date)
    
  }

}