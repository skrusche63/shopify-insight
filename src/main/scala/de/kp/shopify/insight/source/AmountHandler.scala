package de.kp.shopify.insight.source
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

import scala.xml._
import scala.collection.mutable.HashMap

object AmountHandler {
  
  private val path = "amount.xml"
  private val root:Elem = XML.load(getClass.getClassLoader.getResource(path))  

  private val values = load
        
  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  
  private def load:Map[String,String] = {

    val data = HashMap.empty[String,String]   
    /*
     * Time settings for horizon & threshold
     */
    val thoriz_small = (root \ "time" \ "horizon" \ "small").text.trim()
    data += "time.horizon.small" -> thoriz_small

    val thoriz_medium = (root \ "time" \ "horizon" \ "medium").text.trim()
    data += "time.horizon.medium" -> thoriz_medium

    val thoriz_large = (root \ "time" \ "horizon" \ "large").text.trim()
    data += "time.horizon.large" -> thoriz_large

    val tthres_small = (root \ "time" \ "threshold" \ "small").text.trim()
    data += "time.threshold.small" -> tthres_small

    val tthres_medium = (root \ "time" \ "threshold" \ "medium").text.trim()
    data += "time.threshold.medium" -> tthres_medium

    /*
     * Amount settings for horizon & threshold
     */
    val ahoriz_less = (root \ "amount" \ "horizon" \ "less").text.trim()
    data += "amount.horizon.less" -> ahoriz_less

    val ahoriz_equal = (root \ "amount" \ "horizon" \ "equal").text.trim()
    data += "amount.horizon.equal" -> ahoriz_equal

    val ahoriz_large = (root \ "amount" \ "horizon" \ "large").text.trim()
    data += "amount.horizon.large" -> ahoriz_large

    val athres_less = (root \ "amount" \ "threshold" \ "less").text.trim()
    data += "amount.threshold.less" -> athres_less

    val athres_equal = (root \ "amount" \ "threshold" \ "equal").text.trim()
    data += "amount.threshold.equal" -> athres_equal

    data.toMap
    
  }
  /*
   * Time based settings
   */
  protected def SMALL_TIME_HORIZON  = values("time.horizon.small").toInt
  protected def MEDIUM_TIME_HORIZON = values("time.horizon.equal").toInt
  protected def LARGE_TIME_HORIZON  = values("time.horizon.large").toInt

  protected def SMALL_TIME_THRESHOLD  = values("time.threshold.small").toInt
  protected def MEDIUM_TIME_THRESHOLD = values("time.threshold.medium").toInt
  
  /*
   * Amount based settings
   */
  protected def LESS_AMOUNT_HORIZON  = values("amount.horizon.less").toDouble
  protected def EQUAL_AMOUNT_HORIZON = values("amount.horizon.equal").toDouble
  protected def LARGE_AMOUNT_HORIZON = values("amount.horizon.large").toDouble
  
  protected def LESS_AMOUNT_THRESHOLD  = values("amount.threshold.less").toDouble
  protected def EQUAL_AMOUNT_THRESHOLD = values("amount.threshold.equal").toDouble
  /**
   * Amount spent compared to previous transaction
   * 
   * L : significantly less than
   * E : more or less same
   * G : significantly greater than
   * 
   */
  def stateByAmount(next:Float,previous:Float):String = {
    
    if (next < LESS_AMOUNT_THRESHOLD * previous) "L"
     else if (next < EQUAL_AMOUNT_THRESHOLD * previous) "E"
     else "G"
    
  }
  /**   
   * This method translates a period of time, i.e. the time 
   * elapsed since last transaction into 3 discrete states:
   * 
   * S : small, M : medium, L : large
   * 
   */
  def stateByTime(next:Long,previous:Long):String = {
    
    val period = (next -previous) / DAY
    
    if (period < SMALL_TIME_THRESHOLD) "S"
    else if (period < MEDIUM_TIME_THRESHOLD) "M"
    else "L"
  
  }
 
  def nextAmount(nextstate:String,lastamount:Float):Float = {
    
    if (nextstate == "") return 0
    
    lastamount * (
    
        if (nextstate.endsWith("L")) LESS_AMOUNT_HORIZON.toFloat         
        else if (nextstate.endsWith("E")) EQUAL_AMOUNT_HORIZON.toFloat    
        else LARGE_AMOUNT_HORIZON.toFloat
    
    )
    
  }

  def nextDate(nextstate:String,lastdate:Long):Long = {

    if (nextstate == "") return -1
    
    lastdate + DAY * (
    
        if (nextstate.startsWith("S")) SMALL_TIME_HORIZON      
        else if (nextstate.startsWith("M")) MEDIUM_TIME_HORIZON   
        else LARGE_TIME_HORIZON
        
    )

  }

}