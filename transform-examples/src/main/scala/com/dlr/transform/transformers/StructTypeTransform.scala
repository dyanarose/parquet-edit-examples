package com.dlr.transform.transformers

import com.dlr.transform.schemas.RawDataSchema
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, RowFactory, SparkSession}

/**
  * Created by dyana.rose on 04/08/2017.
  */
object StructTypeTransform {

  def transform(spark: SparkSession, sourcePath: String, destPath: String): Unit = {
    val originalData = spark.read.schema(RawDataSchema.schema).parquet(sourcePath)

    // cleanFunc will take the struct as a Row and return a new Row with edited fields
    // note that the ordering and count of the fields must remain the same
    def cleanFunc: (Row => Row) = { r => RowFactory.create(r.getAs[BooleanType](0), "") }

    // register the func as a udf
    // give the UDF a schema or the Row type won't be supported
    val clean = udf(cleanFunc,
      StructType(
        StructField("myField", BooleanType, true) ::
          StructField("editMe", StringType, true) ::
          Nil
      )
    )

    // required for the $ column syntax
    import spark.sqlContext.implicits._

    // if you have data that doesn't need editing, you can separate it out
    // The data will need to be in a form that can be unioned with the edited data
    // I do that here by selecting out all the fields.
    val alreadyGoodData = originalData.where("myStruct.editMe is null").select(
      Seq[Column](
        $"myField",
        $"myStruct",
        $"myMap"
      ):_*
    )

    // apply the udf to the fields that need editing
    // selecting out all the data that will be present in the final parquet file
    val transformedData = originalData.where("myStruct.editMe is not null").select(
      Seq[Column](
        $"myField",
        clean($"myStruct").as("myStruct"),
        $"myMap"
      ):_*
    )

    // union the two DataFrames
    val allGoodData = alreadyGoodData.union(transformedData)

    // write out the final edited data
    allGoodData.write.parquet(destPath)
  }
}
