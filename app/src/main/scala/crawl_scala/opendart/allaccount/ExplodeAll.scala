package crawl_scala.opendart.allaccount

import crawl_scala.opendart.conf.{Paths, Schema}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import whishaw.spark.util.SparkAux

case class FieldFromFileName(
    code: String,
    stockCode: String,
    stockName: String,
    year: String,
    reportCode: String
)

object ExplodeAll {

  def read(spark: SparkSession): DataFrame = {
    spark.read.schema(Schema.allAccountsRaw).json(Paths.allAccountsRaw)
  }

  val processFileName = udf((fileName: String) => {
    val nameTokens = fileName.split("/").last.split("\\.").head.split("_")
    val code = nameTokens(0)
    val stockCode = nameTokens(1)
    val stockName = nameTokens(2)
    val year = nameTokens(3)
    val reportCode = nameTokens(4)

    FieldFromFileName(code, stockCode, stockName, year, reportCode)
  })

  def main(args: Array[String]): Unit = {
    val spark = SparkAux.getSparkSession("ExplodeAll")
    import spark.implicits._

    val df = read(spark)
      .select(
        explode($"list").as("e"),
        processFileName(input_file_name()) as "f"
      )
      .select("e.*", "f.*")
      .repartition(40)
      .persist()

    df.write
      .mode(SaveMode.Overwrite)
      .parquet(Paths.allAccounts)
//    df.write
//      .mode(SaveMode.Overwrite)
//      .json(Paths.allAccountsJson)

    df.select("account_nm")
      .distinct()
      .sort()
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(Paths.accountNamesRaw)

  }

}
