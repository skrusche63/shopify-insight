package de.kp.shopify.insight
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

import org.apache.spark.SparkContext
import akka.actor.ActorRef

class ServerContext(
  /*
   * Reference to the common SparkContext; this context can be used
   * to access HDFS based data sources or leverage the Spark machine
   * learning library or other Spark based functionality
   */
  @transient val sparkContext:SparkContext,
   /*
    * This is a specific actor instance, assigned to the global actor
    * system, that is responsible for receiving any kind of messages
    */
   val listener:ActorRef) extends Serializable {

  /*
   * Determine Shopify access parameters from the configuration file (application.conf).
   * Configuration is the accessor to this file.
   */
  private val (endpoint,apikey,password) = Configuration.shopify
  private val shopifyConfig = new ShopifyConfiguration(endpoint,apikey,password)  
  /*
   * The RemoteContext enables access to remote Akka systems and their actors;
   * this variable is used to access the engines of Predictiveworks
   */
  private val rtx = new RemoteContext()

  /*
   * Heartbeat & timeout configuration in seconds
   */
  private val (heartbeat,time) = Configuration.heartbeat     

  /**
   * The time interval for schedulers (e.g. MonitoredActor or StatusSupervisor) to 
   * determine how often alive messages have to be sent
   */
  def getHeartbeat = heartbeat
  
  def getRemoteContext = rtx
  
  def getShopifyConfig = shopifyConfig
  /*
   * The 'apikey' is used as the 'site' parameter when indexing
   * Shopify data with Elasticsearch
   */
  def getSite = apikey
  /**
   * The timeout interval used to supervise actor interaction
   */
  def getTimeout = time
  
}