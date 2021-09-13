package com.tl.app

import org.apache.spark.sql.types.{StringType, StructType}

object Schemas {
  val wikipediaDataSchema: StructType = new StructType()
    .add("title", StringType, nullable = true)
    .add("url", StringType, nullable = true)
    .add("abstract", StringType, nullable = true)
    .add("links", StringType, nullable = true)
}
