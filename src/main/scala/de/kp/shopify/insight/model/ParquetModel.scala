package de.kp.shopify.insight.model
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

case class ParquetCHN(
  site:String,
  user:String,
  
  amount:Double,
  timespan:Double,
  
  churner:Boolean
)

/**
 * ParquetCLV is a data structure that specifies the customer-specific 
 * lifetime value in terms of an assigned state. The state is determined 
 * by evaluating the respective M_QUANTILES
 */
case class ParquetCLV(
  site:String,
  user:String,
  /* 
   * Total amount spent since the signup date of
   * the customer
   */
  amount:Double,
  state:String
)
/**
 * ParquetFRQ is a data structure that specifies the customer-specific 
 * purchase frequency in terms of an assigned state. The state is determined 
 * by evaluating the respective F_QUANTILES
 */
case class ParquetFRQ(
  site:String,
  user:String,
  /* 
   * Total orders since the signup date of
   * the customer
   */
  total:Int,
  state:String
)
/**
 * ParquetREC is a data structure that specifies the customer-specific 
 * purchase activity (recency) in terms of an assigned state. The state 
 * is determined by evaluating the respective R_QUANTILES
 */
case class ParquetREC(
  site:String,
  user:String,
  /* 
   * The number of days passed since the
   * purchase of the customer
   */
  days:Int,
  state:String
)
/**
 * ParquetPRM is a data structure that specifies product relation
 * rules that form the basis for cross-selling, promotions etc
 */
case class ParquetPRM(
  antecedent:Seq[Int],
  consequent:Seq[Int],
  support:Int,
  total:Long,
  confidence:Double
)
/**
 * ParquetUFM is a data structure that specifies the customer-specific
 * purchase forecasts
 */
case class ParquetUFM(
  site:String,
  user:String,
  
  step:Int,
  
  amount:Double,
  time:Double,
  
  state:String,
  score:Double
)
/**
 * ParquetULM is a data structure that specifies the customer-specific
 * loyalty forecasts
 */
case class ParquetULM(
  site:String,
  user:String,

  trajectory:Seq[String],
  
  low:Double,
  norm:Double,
  high:Double,
  
  rating:Int
)
/**
 * ParquetURM is a data structure that specifies customer-specific
 * product recommendations derived from association rules and the
 * last transaction of the customer
 */
case class ParquetURM(
  site:String,
  user:String,
  recommendations:Seq[(Seq[Int],Double)]
)
/**
 * TODO: Integrate Jollydays project and defines calendars
 * for holidays 
 */

/**
 * ParquetDOW is a data structure that specifies the 
 * customer-specific day of week support and preference; 
 * itcan be used, to e.g. determine which of the customer 
 * is a typical weekend buyer.
 * 
 * This information contributes to the temporal dimension
 * of the customer.
 */
case class ParquetDOW(
  site:String,
  user:String,
  
  day:Int,
  
  supp:Double,
  pref:Double,
  
  total:Int
)
/**
 * ParquetFRP is a data structure that specifies the
 * customer-specific purchase frequency in terms of 
 * days in between two subsequent purchase transactions.
 * 
 * This information contributes to the temporal dimension 
 * of the customer.
 */
case class ParquetFRP(
  site:String,
  user:String,
  /*
   * This profile holds the timestamp of the last purchase 
   * of the customer; this field is used to determine, whether 
   * he or she gets cold
   */
  recency:Long,
  timespan:Int,
  
  avg_timespan:Double,
  min_timespan:Int,
  max_timespan:Int,
  
  supp:Double,
  pref:Double,
  
  total:Int
)
/**
 * ParquetHOD is a data structure that specifies the
 * customer specific hour of the day support and preferences.
 * 
 * This information contributes to the temporal dimension 
 * of the customer.
 */
case class ParquetHOD(
  site:String,
  user:String,
  
  hour:Int,
  
  supp:Double,
  pref:Double,
  
  total:Int
)

case class ParquetITP(
  site:String,
  user:String,
  
  item:Int,
  
  supp:Int,
  pref:Double,
  
  total:Int
)

case class ParquetLOC(
  site:String,
  user:String,
  
  ip_address:String,
  timestamp:Long,
    
  countryname:String,
  countrycode:String,

  region:String,
  regionname:String,
  
  areacode:Int,
  dmacode:Int,
  
  metrocode:Int,
  city:String,
  
  postalcode:String,
	  
  lat:Double,
  lon:Double
)
/**
 * ParquetRFM is a data structure that specifies an e-commerce
 * RFM table, extended by the quantiles for the respective RFM
 * attributes.
 * 
 * Quantiles are used later on to segment customers due to their
 * respective RFM attributes.  
 */
case class ParquetRFM(
  site:String,
  user:String,

  today:Long,
  
  R:Int,
  R_segment:String,
  R_quantiles:String,
  
  F:Int,
  F_segment:String,
  F_quantiles:String,
  
  M:Double,
  M_segment:String,
  M_Quantiles:String
)

/**
 * ParquetASR is a data structure that is shared with Predictiveworks'
 * Association Analysis engine; it is generated by the ASRPreparer and
 * used by the Association Rule Mining algorithm
 */
case class ParquetASR(site:String,user:String,group:String,item:Int)
/**
 * ParquetSTM is a data structure that is shared with Predictiveworks'
 * Intent Recognition engine; it is generated by the STMPreparer and 
 * used by the Markov and Hidden Markov algorithm
 */
case class ParquetSTM(
  site:String,
  user:String,
  
  amount:Float,
  timestamp:Long,
  
  state:String
) 
