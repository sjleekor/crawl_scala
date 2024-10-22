package crawl_scala.opendart.conf

import org.apache.spark.sql.types._

object Schema {

  val allAccountsRaw = StructType(Array(
    StructField("list", ArrayType(StructType(Array(
      StructField("account_detail", StringType, nullable = true),
      StructField("account_id", StringType, nullable = true),
      StructField("account_nm", StringType, nullable = true),
      StructField("bfefrmtrm_amount", StringType, nullable = true),
      StructField("bfefrmtrm_nm", StringType, nullable = true),
      StructField("bsns_year", StringType, nullable = true),
      StructField("corp_code", StringType, nullable = true),
      StructField("currency", StringType, nullable = true),
      StructField("frmtrm_add_amount", StringType, nullable = true),
      StructField("frmtrm_amount", StringType, nullable = true),
      StructField("frmtrm_nm", StringType, nullable = true),
      StructField("frmtrm_q_amount", StringType, nullable = true),
      StructField("frmtrm_q_nm", StringType, nullable = true),
      StructField("ord", StringType, nullable = true),
      StructField("rcept_no", StringType, nullable = true),
      StructField("reprt_code", StringType, nullable = true),
      StructField("sj_div", StringType, nullable = true),
      StructField("sj_nm", StringType, nullable = true),
      StructField("thstrm_add_amount", StringType, nullable = true),
      StructField("thstrm_amount", StringType, nullable = true),
      StructField("thstrm_nm", StringType, nullable = true)
    ))), nullable = true),

    StructField("message", StringType, nullable = true),
    StructField("status", StringType, nullable = true)
  ))


}
