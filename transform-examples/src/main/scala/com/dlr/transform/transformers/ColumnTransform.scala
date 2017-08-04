package com.dlr.transform.transformers

import com.dlr.transform.schemas.{ColumnDropSchema}
import org.apache.spark.sql.{SparkSession}

/**
  * Created by dyana.rose on 04/08/2017.
  */
object ColumnTransform {

  def transform(spark: SparkSession, sourcePath: String, destPath: String): Unit = {

    // read in the data with a new schema
    val allGoodData = spark.read.schema(ColumnDropSchema.schema).parquet(sourcePath)

    // write out the final edited data
    allGoodData.write.parquet(destPath)
  }
}
