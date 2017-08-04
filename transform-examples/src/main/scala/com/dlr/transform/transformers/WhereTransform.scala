package com.dlr.transform.transformers

import com.dlr.transform.schemas.{RawDataSchema}
import org.apache.spark.sql.SparkSession

/**
  * Created by dyana.rose on 04/08/2017.
  */
object WhereTransform {

  def transform(sc: SparkSession, sourcePath: String, destPath: String): Unit = {
    val originalData = sc.read.schema(RawDataSchema.schema).parquet(sourcePath)

    // select only the good data rows
    val allGoodData = originalData.where("myField is null")

    // write out the final edited data
    allGoodData.write.parquet(destPath)
  }
}
