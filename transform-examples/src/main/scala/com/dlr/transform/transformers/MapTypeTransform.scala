package com.dlr.transform.transformers

import com.dlr.transform.schemas.RawDataSchema
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Created by dyana.rose on 04/08/2017.
  */
object MapTypeTransform {

  def transform(sc: SparkSession, sourcePath: String, destPath: String): Unit = {
    val originalData = sc.read.schema(RawDataSchema.schema).parquet(sourcePath)

    // cleanFunc will simply take the MapType and return an edited Map
    // in this example it removes one member of the map before returning
    def cleanFunc: (Map[String, String] => Map[String, String]) = { m => m.filterKeys(k => k != "editMe") }

    // register the func as a udf
    val clean = udf(cleanFunc)

    // required for the $ column syntax
    import sc.sqlContext.implicits._

    // if you have data that doesn't need editing, you can separate it out
    // The data will need to be in a form that can be unioned with the edited data
    // I do that here by selecting out all the fields.
    val alreadyGoodData = originalData.where("myMap.editMe is null").select(
      Seq[Column](
        $"myField",
        $"myMap",
        $"myStruct"
      ):_*
    )

    // apply the udf to the fields that need editing
    // selecting out all the data that will be present in the final parquet file
    val transformedData = originalData.where("myMap.editMe is not null").select(
      Seq[Column](
        $"myField",
        clean($"myMap").as("myMap"),
        $"myStruct"
      ):_*
    )

    // union the two DataFrames
    val allGoodData = alreadyGoodData.union(transformedData)

    // write out the final edited data
    allGoodData.write.parquet(destPath)
  }
}
