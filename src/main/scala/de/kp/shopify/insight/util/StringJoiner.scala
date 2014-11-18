package de.kp.shopify.insight.util
/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

class StringJoiner(separator:String,trimFlag:Int,ignoreNulls:Boolean,leading:Boolean,trailing:Boolean) {

  def this(separator:String) {
    this(separator, -1, false, false, false)
  }

  def ignoreNulls():StringJoiner = {
    return new StringJoiner(separator, trimFlag, true, leading, trailing)
  }

  def leading():StringJoiner = {
     return new StringJoiner(separator, trimFlag, ignoreNulls, true, trailing)
  }

  def trailing():StringJoiner = {
    return new StringJoiner(separator, trimFlag, ignoreNulls, leading, true)
  }

  def trim():StringJoiner = {
    return new StringJoiner(separator, 0, ignoreNulls, leading, trailing)
  }

  def trimToNull():StringJoiner = {
    return new StringJoiner(separator, 1, ignoreNulls, leading, trailing)
  }

  def join(parts:List[String]):String = {
        
    if (parts == null) return null

    val sb = new StringBuilder(parts.size)
    val iter = parts.iterator
    while (iter.hasNext) {
      
      var text:String = iter.next
      if (trimFlag > 0) {
        text = if (trimFlag == 0) trim(text) else trimToNull(text)
      }
      
      if (ignoreNulls && text == null) {
        /* do noting */
      } else {

        sb.append(text)
        if (!iter.hasNext) {
          if (trailing) sb.append(separator)
        
        } else {
          sb.append(separator)
          
        }
        
      }
      
    }
//        for (Iterator<String> iter = parts.iterator(); iter.hasNext();) {
//
//        }

    if (sb.length() == 0 && trimFlag == 1)return null
    if (leading)sb.insert(0, separator)

    return sb.toString()
  
  }

  private def trim(string:String):String = {
    if (string == null) null else string.trim()
  }

  private def trimToNull(string:String):String = {
   
    if (string == null) return null
    val text = string.trim()
   
    if (text.length() == 0) null else text
 
  }

  def joiner(separator:String):StringJoiner = {
    return new StringJoiner(separator)
  }
  
}