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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import de.kp.shopify.insight.model._

object TFIDF {

  /**
   * This method computes the user item preference with an algorithm
   * that is similar to TF-IDF from text mining
   */
  def compute(dataset:RDD[(String,String,Int,Int,String)]):RDD[ParquetCPA] = {
    
    val sc = dataset.sparkContext
    /*
     * A document is equivalent to a user in this approach, and thus 
     * we calculate the total number of users: to this end, we have
     * to group the dataset by (site,user)
     */
    val ds = dataset.groupBy(x => (x._1,x._2))
    val total = sc.broadcast(ds.count)
    /*
     * Next we compute the inverse user (document) frequency, i.e. the
     * number of documents is equal to the number of users, and, the
     * occurrence of an item with respect to the users that purchased
     * the item.
     * 
     * In order to build vectors from this information, we also need
     * to zip the idf data structure with an index to enable vector
     * based operations on top of the TFIDF user item preference.
     * 
     * Note, that the index assigned here starts with 0 and counts
     * all elements event if the dataset is spread across multiple
     * partitions 
     */
    val indexed_idf = dataset.groupBy(x => x._3).map(x => {
      
      val item = x._1
      /*
       * The data contains all users and the purchase quantity
       * for each transaction that was made by the user.
       * 
       *  For an IDF calculation, we are interested in the number
       *  of customers that have purchased a certain item 
       */
      val supp = x._2.map(v => (v._1,v._2)).toSeq.distinct.size
      val iidf = Math.log( total.value / 1 + supp)
      
      (item,iidf)
      
    }).zipWithIndex.map(x => (x._1._1,(x._1._2,x._2)))
    
    /*
     * Next we compute the normalized item frequency with respect
     * to a certain user (document)
     */
    val tdf = dataset.groupBy(x => (x._1,x._2)).flatMap(x => {
      
      val (site,user) = x._1
      /*
       * Determine the (item,quantity) list for a certain
       * user; note, that a certain item can appear twice
       * if the customer purchased this item in more than
       * one transaction
       */
      val isup = x._2.map(v => (v._3,v._4,v._5)).groupBy(v => v._1).map(v => {
        
        val item = v._1
        /*
         * The product category is a denormalized attribute;
         * this implies that we only have to extract it from
         * the head dataset
         */
        val category = v._2.head._3
        val frequency = v._2.map(_._2).sum
        
        (item,frequency,category)
      
      })
      /*
       * Normalize the item frequency with respect to the 
       * maximum item support for the respective user
       */
      val max = isup.map(_._2).max
      isup.map(v => {
        
        val item = v._1
        val category = v._3
        
        val itdf = Math.log(1 + v._2.toDouble / max)
        
        (item,(site,user,itdf,category))
        
      })
      
    })
    
    /*
     * The TF-IDF score is computed by joining the 'tf' and 'idf'
     * data structure with 'item' as key and then multiply the
     * respective values; in addition a 'row' information is added
     * by indexing the dataset. This is done with respect to the
     * subsequent vector based processing of the customer product
     * affinity
     */
    tdf.join(indexed_idf).map(x => {
      
      val item = x._1
      val ((site,user,itdf,category),(iidf,pos)) = x._2
      /*
       * The 'pos' variable describes the vector position of the 
       * data record with respect to ALL items under consideration
       */
      (site,user,item,pos,itdf * iidf,category)
      
    }).zipWithIndex.map(x => {
      
      val row = x._2
      val (site,user,item,col,value,label) = x._1

      ParquetCPA(
        site,
        user,  
        item,
        row,
        col,
        label,
        value
      )
      
    })
  
  }
  
}