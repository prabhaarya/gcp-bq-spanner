//      Copyright 2024 Google LLC
//
//      Licensed under the Apache License, Version 2.0 (the "License");
//      you may not use this file except in compliance with the License.
//      You may obtain a copy of the License at
//
//          http://www.apache.org/licenses/LICENSE-2.0
//
//      Unless required by applicable law or agreed to in writing, software
//      distributed under the License is distributed on an "AS IS" BASIS,
//      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//      See the License for the specific language governing permissions and
//      limitations under the License.

import org.apache.spark.sql.SparkSession
import com.google.cloud.spark.bigquery._
import scala.collection.JavaConverters._
import java.util.ArrayList
import com.google.cloud.spanner.{DatabaseClient, Mutation, SpannerOptions, Value}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.math.{BigDecimal => SqlBigDecimal}
import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}
import com.google.cloud.{Date => CloudDate, Timestamp => CloudTimestamp}
import scala.collection.mutable.ArrayBuffer


object spanner_mutations {

  def execute_pipeline() = {
    val spark = SparkSession.builder()
      .appName("spark-spanner-demo")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    val old_df = spark.read.bigquery("prabha-poc.DATAHUB_01B.yellow_trips_enhanced")
    old_df.createOrReplaceTempView("old_df")
    val df = spark.sql("SELECT * FROM old_df")
    df.printSchema()
    val projectId = "prabha-poc"
    val instanceId = "test-instance"
    val databaseId = "example-db"

    df.rdd.foreachPartition { partition =>
      val client = createSpannerClient(projectId, instanceId, databaseId)
      val mutations = ArrayBuffer.empty[Mutation]

      partition.foreach { row =>
        mutations += buildMutation(row)

//        Batches 100 mutations into a single transaction when writing to Spanner
        if (mutations.size >= 100) {
          client.write(mutations.asJava) // Pass the buffer directly
          mutations.clear()
        }
      }

      if (mutations.nonEmpty) {
        client.write(mutations.asJava) // Pass the buffer directly
      }
    }

    spark.stop()
  }

  // Helper function to create Spanner client
  def createSpannerClient(projectId: String, instanceId: String, databaseId: String): DatabaseClient = {
    val options = SpannerOptions.newBuilder().build()
    val spanner = options.getService()
    spanner.getDatabaseClient(com.google.cloud.spanner.DatabaseId.of(projectId, instanceId, databaseId))
  }

  // Helper function to do conversion of data types
  def convertSqlDateToCloudDate(sqlDate: SqlDate): CloudDate = {
    val localDate = sqlDate.toLocalDate
    CloudDate.fromYearMonthDay(localDate.getYear, localDate.getMonthValue, localDate.getDayOfMonth)
  }
  def convertSqlTimestampToCloudTimestamp(SqlTimestamp: SqlTimestamp): CloudTimestamp = {
    CloudTimestamp.of(SqlTimestamp)
  }

  // Function to extract BigDecimal values (with null handling)
  def convertBigDecimalToSpannerFloat(row: Row, columnName: String): Option[Double] = {
    Option(row.getAs[java.math.BigDecimal](columnName)).map(_.doubleValue())
  }

  // Generic function to extract values (with null handling)
  def getValue[T](row: Row, columnName: String)(implicit evidence: Manifest[T]): Option[T] = {
    Option(row.getAs[T](columnName))
  }

  // Function to build mutation (adjust based on your schema)
  def buildMutation(row: Row): Mutation = {
    /*
    The buildMutation function constructs a single Mutation object for each row of data extracted from the BigQuery table.
     */
    val vendorID = getValue[String](row, "vendor_id").getOrElse("")
    val pickupDatetime = convertSqlTimestampToCloudTimestamp(row.getAs[SqlTimestamp]("pickup_datetime"))
    val dropoffDatetime = convertSqlTimestampToCloudTimestamp(row.getAs[SqlTimestamp]("dropoff_datetime"))
    //    val passengerCount = row.getAs[Long]("passenger_count")
    val passengerCount = getValue[Long](row, "passenger_count").getOrElse(0L)
    val tripDistance = convertBigDecimalToSpannerFloat(row, "trip_distance").getOrElse(0.0)
    val rateCode = getValue[String](row, "rate_code").getOrElse("")
    val storeAndFwdFlag = getValue[String](row, "store_and_fwd_flag").getOrElse("")
    val paymentType = getValue[String](row, "payment_type").getOrElse("")
    val fareAmount =  convertBigDecimalToSpannerFloat(row, "fare_amount").getOrElse(0.0)
    val extra =  convertBigDecimalToSpannerFloat(row, "extra").getOrElse(0.0)
    val mtaTax =  convertBigDecimalToSpannerFloat(row, "mta_tax").getOrElse(0.0)
    val tipAmount =  convertBigDecimalToSpannerFloat(row, "tip_amount").getOrElse(0.0)
    val tollsAmount =  convertBigDecimalToSpannerFloat(row, "tolls_amount").getOrElse(0.0)
    val impSurcharge =  convertBigDecimalToSpannerFloat(row, "imp_surcharge").getOrElse(0.0)
    val airportFee =  convertBigDecimalToSpannerFloat(row, "airport_fee").getOrElse(0.0)
    val totalAmount =  convertBigDecimalToSpannerFloat(row, "total_amount").getOrElse(0.0)
    val pickupLocationId = getValue[String](row, "pickup_location_id").getOrElse("")
    val dropoffLocationId = getValue[String](row, "dropoff_location_id").getOrElse("")
    val dataFileYear = getValue[Long](row, "data_file_year").getOrElse(0L)
    val dataFileMonth = getValue[Long](row, "data_file_month").getOrElse(0L)
    val tripDate = convertSqlDateToCloudDate(row.getAs[SqlDate]("trip_date"))
    val modNum = getValue[Long](row, "mod_num").getOrElse(0L)
    val label = getValue[String](row, "label").getOrElse("")

    Mutation.newInsertOrUpdateBuilder("yellow_trips_enhanced_1")
      .set("vendor_id").to(vendorID)
      .set("pickup_datetime").to(pickupDatetime)
      .set("dropoff_datetime").to(dropoffDatetime)
      .set("passenger_count").to(passengerCount)
      .set("trip_distance").to(tripDistance)
      .set("rate_code").to(rateCode)
      .set("store_and_fwd_flag").to(storeAndFwdFlag)
      .set("payment_type").to(paymentType)
      .set("fare_amount").to(fareAmount)
      .set("extra").to(extra)
      .set("mta_tax").to(mtaTax)
      .set("tip_amount").to(tipAmount)
      .set("tolls_amount").to(tollsAmount)
      .set("imp_surcharge").to(impSurcharge)
      .set("airport_fee").to(airportFee)
      .set("total_amount").to(totalAmount)
      .set("pickup_location_id").to(pickupLocationId)
      .set("dropoff_location_id").to(dropoffLocationId)
      .set("data_file_year").to(dataFileYear)
      .set("data_file_month").to(dataFileMonth)
      .set("trip_date").to(tripDate)
      .set("mod_num").to(modNum)
      .set("label").to(label)
      .build()
  }
}
