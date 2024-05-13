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
              
    // Calculate dynamic batch size based on number of columns
    val numColumns = df.schema.fields.length
    val batchSize = Math.max(100 / numColumns, 1) // Adjust divisor as needed
    print("batchSize-->", batchSize)

    df.rdd.foreachPartition { partition =>
      val client = createSpannerClient(projectId, instanceId, databaseId)
      val mutations = ArrayBuffer.empty[Mutation]

      partition.foreach { row =>
        mutations += buildMutation(row, df) // Pass DataFrame to buildMutation

//        Batches 100 mutations into a single transaction when writing to Spanner
        if (mutations.size >= batchSize) {
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
          
// Function to build mutation dynamically
def buildMutation(row: Row, df: DataFrame): Mutation = {
    val builder = Mutation.newInsertOrUpdateBuilder("yellow_trips_enhanced_1")

    df.schema.fields.foreach { field =>
    val columnName = field.name
    val columnType = field.dataType

    val spannerValue: Value = columnType match { // Specify Value as the return type
        case org.apache.spark.sql.types.StringType => Value.string(getValue[String](row, columnName).getOrElse(""))
        case org.apache.spark.sql.types.LongType => Value.int64(getValue[Long](row, columnName).getOrElse(0L))
        case org.apache.spark.sql.types.TimestampType => Value.timestamp(convertSqlTimestampToCloudTimestamp(row.getAs[SqlTimestamp](columnName)))
        case org.apache.spark.sql.types.DateType => Value.date(convertSqlDateToCloudDate(row.getAs[SqlDate](columnName)))
        case org.apache.spark.sql.types.DecimalType() => Value.float64(convertBigDecimalToSpannerFloat(row, columnName).getOrElse(0.0))
        // Add other data type conversions as needed
        case _ => throw new IllegalArgumentException(s"Unsupported data type: ${columnType}")
    }

    builder.set(columnName).to(spannerValue) // Use the Spanner Value
    }

    builder.build()

  }
}
