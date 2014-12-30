package de.kp.shopify.insight.preference
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

import org.joda.time.DateTime

class TimePreference {

  /**
   * This method builds the purchase preference time from the
   * customers' purchase history
   */
  def build(rawset:Array[(String,String,String,Long,Int)]):List[(String,String,List[(Int,Int)],List[(Int,Int)])] = {
    /*
     * STEP #1: Group all the items by group, independent of
     * the respective customers and determine the timestamp
     * assigned to the group; from this timestamp compute the
     * support for a certain day of the week and a specific
     * time period of the day
     */
    val timestamps = rawset.groupBy(x => x._3).map(x => (x._1,x._2(0)._4))
    
    val total = timestamps.size
    /* 1..7 */
    val globalDaySupp = timestamps.map(x => {
        
        val (group,timestamp) = x

        val date = new java.util.Date()
        date.setTime(timestamp)
      
        val datetime = new DateTime(date)
        val dow = datetime.dayOfWeek().get
        
        (dow,group)
       
    }).groupBy(x => x._1).map(x => (x._1,x._2.size))
    /* 1..4 */
    val globalTimeSupp = timestamps.map(x => {
        
        val (group,timestamp) = x

        val date = new java.util.Date()
        date.setTime(timestamp)
      
        val datetime = new DateTime(date)
        val hod = datetime.hourOfDay().get
        
        (timeOfDay(hod),group)
       
    }).groupBy(x => x._1).map(x => (x._1,x._2.size))
    
    /*
     * STEP #2: Compute the preference for a certain day of the
     * week and also a specific time period of the day.
     * 
     * The preference is computed as follows:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     * 
     */
     val globalDayPref = globalDaySupp.map(x => {
       
       val (day,supp) = x 
       val pref = Math.log(1 + supp.toDouble / total.toDouble)    
       
       (day,pref)
       
     })
     
     val globalTimePref = globalTimeSupp.map(x => {
       
       val (time,supp) = x 
       val pref = Math.log(1 + supp.toDouble / total.toDouble)    
       
       (time,pref)
       
     })
    
    /*
     * STEP #3: After having calculated the overall preferences
     * for a certain day of the week and a time period of the day,
     * we can compute the respective data for each (site,user)
     */
    rawset.groupBy(x => (x._1,x._2)).map(x => {
      
      val (site,user) = x._1
      /* 
       * Reduce value to (group,timestamp,item,score), group by 'group', 
       * which describes a certain order or transaction and extract the
       * first entry as all items of a certain transaction refer to the
       * same timestamp.
       * 
       * The result specifies a list of (order,timestamp) for a certain
       * (uid,site,user) tuple
       */
      val user_timestamps = x._2.groupBy(x => x._3).map(x => (x._1,x._2(0)._4))
      val user_total = user_timestamps.size
      /*
       * Compute the support for a certain day of the
       * week and a specific time period of the day.
       */      
      val userDaySupp = user_timestamps.map(x => {
        
        val (group,timestamp) = x

        val date = new java.util.Date()
        date.setTime(timestamp)
      
        val datetime = new DateTime(date)
        val dow = datetime.dayOfWeek().get
        
        (dow,group)
        
      }).groupBy(x => x._1).map(x => (x._1,x._2.size))
      
      val userTimeSupp = user_timestamps.map(x => {
        
        val (group,timestamp) = x

        val date = new java.util.Date()
        date.setTime(timestamp)

        val datetime = new DateTime(date)
        val hod = datetime.hourOfDay().get
        
        (timeOfDay(hod),group)
        
      }).groupBy(x => x._1).map(x => (x._1,x._2.size))
      
      /*
       * Compute the preference for a certain day of the
       * week and also a specific time period of the day.
       * 
       * The preference is computed as follows:
       * 
       * pref = Math.log(1 + supp.toDouble / total.toDouble)
       * 
       */
      val userDayPref = userDaySupp.map(x => {
       
        val (day,supp) = x 
        val pref = Math.log(1 + supp.toDouble / total.toDouble)    

        /*
         * Normalize the user preference with respect to the
         * global day preference and assign a preference value
         * from 1..5
         */
        val gpref = globalDayPref(day)
        val npref = Math.round( 5* (pref.toDouble / gpref.toDouble) ).toInt

        (day,npref)
       
      }).toList
     
      val userTimePref = userTimeSupp.map(x => {
       
        val (time,supp) = x 
        val pref = Math.log(1 + supp.toDouble / total.toDouble)    
        /*
         * Normalize the user preference with respect to the
         * global time preference and assign a preference value
         * from 1..5
         */
        val gpref = globalTimePref(time)
        val npref = Math.round( 5* (pref.toDouble / gpref.toDouble) ).toInt
       
        (time,npref)
       
      }).toList
        
      (site,user,userDayPref,userTimePref)
      
    }).toList
    
  }
  
  private def timeOfDay(hour:Int):Int = {

    val timeOfDay = if (0 < hour && hour < 7) {
      /*
       * The period from midnight, 0, to 6
       * specifies the period before work
       */
      1
    
    } else if (7 <= hour && hour < 13) {
      /*
       * The period from 7 to 12 specifies
       * the morning period of a day
       */
      2
    
    } else if (13 <= hour && hour < 18) {
      /*
       * The period from 13 to 17 specifies
       * the afternoon period of a day
       */
      3
    
    } else {
      /*
       * The period from 18 to 23 specifies
       * the evening periof of a day
       */
      4
    }

    timeOfDay
    
  }

}