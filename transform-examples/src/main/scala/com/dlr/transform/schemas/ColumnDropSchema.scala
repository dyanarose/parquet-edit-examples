package com.dlr.transform.schemas

import org.apache.spark.sql.types._

/**
  * Created by dyana.rose on 04/08/2017.
  */
object ColumnDropSchema {
  def schema = StructType(
    StructField("myField", StringType, true) ::
    StructField("myStruct", StructType(
        StructField("myField", BooleanType, true) ::
        StructField("editMe", StringType, true) :: Nil
      )
    ):: Nil
  )
}
