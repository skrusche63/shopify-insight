package de.kp.shopify.insight.learn
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

class LearnerService(val appName:String) extends SparkService {
  
  protected val sc = createCtxLocal("BuildContext",Configuration.spark)      
  protected val system = ActorSystem("BuildSystem")

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
    
    val uid = parser.option[String](List("uid"),"uid","Unique job identifier")
    val job = parser.option[String](List("job"),"job","Unique job descriptor")

    parser.parse(args)
    
    /* Validate parameters */
    if (uid.hasValue == false)
      throw new Exception("Parameter 'uid' is missing.")
    
    if (job.hasValue == false)
      throw new Exception("Parameter 'job' is missing.")
  
    val jobs = List("ASR","CDA","CHA","CPA","STM")
    if (jobs.contains(job.value.get) == false)
      throw new Exception("Job parameter must be one of [ASR, CDA, CHA, CPA, STM].")
 
    /* Collect parameters */
    val params = HashMap.empty[String,String]
     
    params += "uid" -> uid.value.get
    params += "job" -> job.value.get
    
    params += "timestamp" -> new DateTime().getMillis.toString

    params.toMap
    
  }
  
  protected def initialize(params:Map[String,String]) {
    /*
     * Create Elasticsearch database and register 
     * the respective task in the task database
     */
    createESIndex(params)
    registerESTask(params)
    
  }

  private def registerESTask(params:Map[String,String]) = {
    
    val key = "BUILD:" + params(Names.REQ_NAME) + ":" + params(Names.REQ_UID)
    val task = "Data mining and model building with " + appName + "."
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
    
  }
  
}