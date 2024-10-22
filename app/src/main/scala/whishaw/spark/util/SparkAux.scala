package whishaw.spark.util

object SparkAux {

  def getSparkSession(appName: String): org.apache.spark.sql.SparkSession = {
    org.apache.spark.sql.SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
  }

}
