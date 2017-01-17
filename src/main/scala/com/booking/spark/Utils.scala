package com.booking.spark

object Utils {
  def toJson(stringMap: Map[String, String]): String = {
    "{" + stringMap.map( x => {
      val k = x._1
      val v = x._2
      s"""\"$k\": \"$v\""""
    }).mkString(",") + "}"
  }
}
