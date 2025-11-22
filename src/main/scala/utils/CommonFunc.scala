package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonFunc {
  def lecturaCSV(path:String, sc: SparkContext) = {
    sc.textFile(path)
  }

  def lecturaCSVDF(path: String)(implicit ss: SparkSession): DataFrame = {
    ss.read.csv(path)
  }

  def lecturaCSVDFHeaders(path: String, delimiter: String)(implicit ss: SparkSession): DataFrame = {
    //    ss.read.option("header",true).option("delimiter", delimiter).csv(path)
    ss.read.options(Map("header" -> "true", "delimiter" -> delimiter)).csv(path)
  }

  def writeCSV(df: DataFrame, path:String) = {
    df.write.option("header",true).mode("overwrite").csv(path)

  }
}
