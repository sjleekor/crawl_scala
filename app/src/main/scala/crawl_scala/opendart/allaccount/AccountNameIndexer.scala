package crawl_scala.opendart.allaccount

import crawl_scala.opendart.conf.Paths
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import whishaw.spark.util.SparkAux

import scala.io.Source

case class SequenceIndex(strSeq: Seq[String], indexSeq: Seq[Int])
case class AccountNameIndex(
    accountName: String,
    strSeq: Seq[String],
    indexSeq: Seq[Int]
)

object AccountNameIndexer {

  def main(args: Array[String]): Unit = {
    val spark = SparkAux.getSparkSession("AccountNameIndexer")
    import spark.implicits._

    val rawStringSchema = StructType(
      Array(
        StructField("str", StringType, nullable = true)
      )
    )

    val rawStrings = spark.read
      .schema(rawStringSchema)
      .csv(Paths.accountNamesRaw)
      .as[String]
      .collect()

    val tempJsonLinesSchema = StructType(
      Array(
        StructField(
          "seq",
          ArrayType(
            StructType(
              Array(
                StructField("str", StringType, nullable = true)
              )
            )
          ),
          nullable = true
        )
      )
    )

    val rdd = spark.sparkContext
      .textFile(Paths.accountNameDhaToken)
      .map { x => """{"seq":""" + x + """}""" }
    val df = rdd.toDF("raw")

    val toSeq = udf { (s: Seq[Row]) =>
      {
        s.map { r => r.getString(0) }
      }
    }

    val stringSequences =
      df.select(from_json($"raw", tempJsonLinesSchema) as "json")
        .select(toSeq($"json.seq") as "strSeq")

    val indexMap =
      stringSequences
        .select(explode($"strSeq") as "str")
        .groupBy($"str")
        .agg(count("*") as "count")
        .filter($"count" > 1)
        .orderBy($"count".desc)
        .select($"str")
        .as[String]
        .collect()
        .zipWithIndex
        .toMap

    val indexUdf = udf((seq: Seq[String]) => {
      seq.map { s =>
        indexMap.getOrElse(s, -1)
      }
    })

    val seqs =
      stringSequences
        .select(
          $"strSeq",
          indexUdf($"strSeq") as "indexSeq"
        )
        .as[SequenceIndex]
        .collect()

    val zipped = rawStrings.zip(seqs)
    val indexSeqResult = {
      zipped.map { case (raw, seq) =>
        AccountNameIndex(raw, seq.strSeq, seq.indexSeq)
      }.toList
    }

    val resultDS = spark.createDataset(indexSeqResult)

    resultDS.write
      .mode(SaveMode.Overwrite)
      .parquet(Paths.accountNameIndex)

    resultDS.write
      .mode(SaveMode.Overwrite)
      .json(Paths.accountNameIndexJson)

  }

}
