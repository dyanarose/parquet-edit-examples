package com.dlr.transform.transformers

import com.dlr.transform.schemas.RawDataSchema
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.udf

/**
  * Created by dyana.rose on 04/08/2017.
  */
object SimpleTransform {

  def transform(spark: SparkSession, sourcePath: String, destPath: String): Unit = {
    val originalData = spark.read.schema(RawDataSchema.schema).parquet(sourcePath)

    // take in a String, return a String
    // cleanFunc will simply take the StringType field value and return the empty string in its place
    // you can interrogate the value and return any StringType here
    def cleanFunc: (String => String) = { _ => "" }

    // register the func as a udf
    val clean = udf(cleanFunc)

    // required for the $ column syntax
    import spark.sqlContext.implicits._

    // if you have data that doesn't need editing, you can separate it out
    // The data will need to be in a form that can be unioned with the edited data
    // I do that here by selecting out all the fields.
    val alreadyGoodData = originalData.where("myField is null").select(
      Seq[Column](
        $"myField",
        $"myMap",
        $"myStruct"
      ):_*
    )

    // apply the udf to the fields that need editing
    // selecting out all the data that will be present in the final parquet file
    val transformedData = originalData.where("myField is not null").select(
      Seq[Column](
        clean($"myField").as("myField"),
        $"myMap",
        $"myStruct"
      ):_*
    )

    // union the two DataFrames
    val allGoodData = alreadyGoodData.union(transformedData)

    // write out the final edited data
    allGoodData.write.parquet(destPath)
  }
}
