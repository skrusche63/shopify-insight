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

import java.io.IOException

import javax.ws.rs.HttpMethod
import javax.ws.rs.client.{Client,ClientBuilder,Entity,WebTarget}
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import de.kp.shopify.insight.model._
import de.kp.shopify.insight.util.StringJoiner

import org.slf4j.{Logger,LoggerFactory}
import de.kp.shopify.insight.model.ShopifyRequest

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

class ShopifyClient(configuration:ShopifyConfiguration) {

  private val LOG = LoggerFactory.getLogger(classOf[ShopifyClient])

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)
  
  val client = ClientBuilder.newClient()
  val endpoint = configuration.getEndpoint().replaceFirst("://", "://" + 
      configuration.getApikey() + ":" + configuration.getPassword() + "@")
      
  val webTarget = client.target(endpoint).path("admin")   
  
  def getProductVariant(sku:String):ShopifyProductVariant = {
    
    val query = new ShopifyQueryBuilder().withFields(List("id", "sku")).build()
    val response = getResponse("variants.json", query, null, HttpMethod.GET)
    
    val variants = response.productVariants
    for (variant <- variants) {
      if (sku.equals(variant.sku)) {
        return getProductVariant(variant.id)
      }
      
    }
    
    null
    
    }

  def getProductVariant(productVariantId:Long):ShopifyProductVariant = {
    getResponse("variants/" + productVariantId + ".json", null, null, HttpMethod.GET).productVariant
  }

  def updateProductVariant(productVariant:ShopifyProductVariant):ShopifyProductVariant = {
    val request = new ShopifyRequest(productVariant)
    getResponse("variants/" + productVariant.id + ".json", null, request, HttpMethod.PUT).productVariant
  }

  def getOrder(orderId:Long):ShopifyOrder = {
    getResponse("orders/" + orderId + ".json", null, null, HttpMethod.GET).order
  }

  def queryOrders(query:ShopifyQuery):List[ShopifyOrder] = {
    getResponse("orders.json", query, null, HttpMethod.GET).orders
  }

  def closeOrder(orderId:Long):ShopifyOrder = {
    getResponse("orders/" + orderId + "/close.json", null, null, HttpMethod.POST).order
  }

  private def getResponse(resourcePath:String,query:ShopifyQuery,request:ShopifyRequest,method:String):ShopifyResponse = {

    val parameterMap = HashMap.empty[String,String]
       
    try {
      
      var queryTarget = webTarget.path(resourcePath)
      if (query != null) {
        
        if (query.fields != null) {
          parameterMap += "fields" -> query.fields.mkString(",")
        }

        if (query.sinceId != -1) {
          parameterMap += "since_id" -> query.sinceId.toString
        }

        for (entry <- parameterMap) {
          val (k,v) = entry
          queryTarget = queryTarget.queryParam(k,v)
        }

      }

      val params = parameterMap.toMap
      val message = String.format("""Request parameters: %s %s""",resourcePath,params)
      
      LOG.info(message)

      val jsonRequest:String = if (request != null) {
         
        val body = JSON_MAPPER.writeValueAsString(request)         
        LOG.info(String.format("""Request body: %s""", body))
         
         body
         
      } else null


      val jsonResponse = queryTarget.request(MediaType.APPLICATION_JSON_TYPE)
                           .method(method, if (jsonRequest == null) null else Entity.json(jsonRequest), classOf[String])

      LOG.info("Response body: " + jsonResponse)

      val response = JSON_MAPPER.readValue(jsonResponse, classOf[ShopifyResponse])
      validate(response)
            
      return response
    
    } catch {
      case e:Exception => throw new ShopifyException("Could not process query",e)
    }

  }

  private def validate(response:ShopifyResponse) {
    if (response.errors != null) {
      throw new ShopifyException(response.errors)
    }
  }

}