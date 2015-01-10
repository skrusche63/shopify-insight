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

/**********************************************************************
 *      
 *                       SUB PROCESS 'PREPARE'
 * 
 *********************************************************************/

/**
 * ParquetRFM is a data structure that specifies a marketing RFM table
 */
case class ParquetRFM(
  site:String,
  user:String,

  today:Long,
  /*
   * recency, frequency and monetary describe the original
   * values extracted from the orders under consideration
   */
  recency:Int,
  frequency:Int,
  monetary:Double,
  /*
   * rval, fval and mval specifies the original values with
   * respect to a quantiles (5) distribution, where the vals
   * are between 1..5, and 5 indicates the highest value for
   * the respective business company
   */
  rval:Int,  
  fval:Int,  
  mval:Int,
  /*
   * rfm_type divides the customer RFM space into 8 different
   * customer categories, 1..8, where 1 indicates the most
   * valuable customer for the respective business company.
   * 
   * The rfm_type is derived from the average values for R, F
   * and M and assigned the state, H or L, if the respective
   * value is above or below the average value
   */
  rfm_type:Int
)
/**
 * ParquetCST is a data structure that specifies a Parquet
 * table that assigns customers to a certain customer type.
 * 
 * The customer type (1..8) is the main segmentation mechanism
 * and used to apply data mining and model building to datasets
 * for certain customer types. E.g. '1' specifies the most
 * valuable customer type, and purchase forecast modeling is 
 * performed with respect to these different types.
 * 
 * It makes definitely no sense to e.g. build a state transition
 * model for customers that exist in completely different sub
 * spaces of the RFM space. 
 */
case class ParquetCST(
  site:String,
  user:String,
  rfm_type:Int
)
/**
 * ParquetIPF is a data structure that specifies a Parquet
 * file that assigns frequencies to a certain product or 
 * item. The data record can be computed with respect to
 * a certain customer type (rfm_type)
 */
case class ParquetIPF(
  item:Int,
  /* The customer and purchase frequency */
  customer:Int,
  purchase:Int,
  /*
   * cval and pval speciy the original values with respect 
   * to a quantiles (5) distribution, where the vals are 
   * between 1..5, and 5 indicates the highest value for
   * the respective business company
   */
  cval:Int,
  pval:Int,
  /* 
   * The customer type the customer and purchase 
   * segmentation is referred to
   */
  rfm_type:Int
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
  
  amount:Double,
  timestamp:Long,
  
  /*
   * We need to provide the quantile boundaries for amount ratio
   * and timespan with the customer specific state description 
   * in order to re-interpret the predicted states in terms of
   * amounts and timespans
   */
  r_b1:Double,
  r_b2:Double,
  r_b3:Double,
  r_b4:Double,
  r_b5:Double,
  
  s_b1:Double,
  s_b2:Double,
  s_b3:Double,
  s_b4:Double,
  s_b5:Double,
  
  /*
   * This is the state specification of the respective customer
   * amount and timespan; note, that the state refers to the
   * timestamp above
   */
  state:String

) 
