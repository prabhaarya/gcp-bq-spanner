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
import com.google.cloud.spanner.{
  DatabaseClient,
  Mutation,
  SpannerOptions,
  Value
}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.math.{BigDecimal => SqlBigDecimal}
import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}
import com.google.cloud.{Date => CloudDate, Timestamp => CloudTimestamp}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.LongAccumulator

object spanner_mutations {

  def execute_pipeline() = {
    val spark = SparkSession
      .builder()
      .appName("spark-spanner-demo")
      .config("spark.master", "local")      
      .config("spark.executor.memory", "8g") // Executor memory (adjust as needed)
      .config("spark.driver.memory", "4g") // Driver memory (adjust as needed)
      .config("spark.executor.instances", "13") // Number of executor instances
      .config("spark.executor.cores", "2") // Number of cores per executor (adjust based on available resources)
      .getOrCreate()
    import spark.implicits._

    val old_df =
      spark.read.bigquery("prabha-poc.DATAHUB_01B.yellow_trips_enhanced")
    old_df.createOrReplaceTempView("old_df")
    val df = spark.sql("SELECT * FROM old_df")
    df.printSchema()
    val projectId = "prabha-poc"
    val instanceId = "test-instance"
    val databaseId = "example-db"
    
    // Get the DataFrame schema on the driver
    val schema = df.schema
    
    // Create an accumulator to track total mutations
    val totalMutations = spark.sparkContext.longAccumulator("Total Mutations")

    val maxMutationSizeInBytes = 2 * 1024 * 1024 // 2MB
    val maxMutationsPerTransaction = 10000 // Spanner's limit, adjust if needed

    // Estimate average row size in bytes (this is a simplified estimation)
    val estimatedRowSizeInBytes = df.schema.fields.map { field =>
      field.dataType match {
        case org.apache.spark.sql.types.StringType =>
          256 // Assume average string length
        case org.apache.spark.sql.types.LongType      => 8
        case org.apache.spark.sql.types.TimestampType => 8
        case org.apache.spark.sql.types.DateType      => 4
        case org.apache.spark.sql.types.DecimalType() => 8
        // Add estimations for other data types
        case _ => 256 // Default to a larger size for unknown types
      }
    }.sum

    // Calculate batch size
    val batchSize = Math
      .min(
        maxMutationSizeInBytes / estimatedRowSizeInBytes, // Limit by mutation size
        maxMutationsPerTransaction
      )
      .toInt

    df.rdd.foreachPartition { partition =>
      val client = createSpannerClient(projectId, instanceId, databaseId)
      val mutations = ArrayBuffer.empty[Mutation]

      partition.foreach { row =>
        mutations += buildMutation(
          row,
          schema
        ) // Pass DataFrame to buildMutation
        totalMutations.add(1) // Increment accumulator

        // Use the dynamic batch size for mutations into single transaction to write spanner
        // Check if accumulated mutations reached the batch size
        if (totalMutations.value >= batchSize) {
          client.write(mutations.asJava) // Pass the buffer directly
          mutations.clear()
          totalMutations.reset() // Reset accumulator after write
        }
      }

      // Write remaining mutations after processing partition
      if (mutations.nonEmpty) {
        client.write(mutations.asJava) // Pass the buffer directly
      }
    }

    spark.stop()
  }

  // Helper function to create Spanner client
  def createSpannerClient(
      projectId: String,
      instanceId: String,
      databaseId: String
  ): DatabaseClient = {
    val options = SpannerOptions.newBuilder().build()
    val spanner = options.getService()
    spanner.getDatabaseClient(
      com.google.cloud.spanner.DatabaseId.of(projectId, instanceId, databaseId)
    )
  }

  // Helper function to do conversion of data types
  def convertSqlDateToCloudDate(sqlDate: SqlDate): CloudDate = {
    val localDate = sqlDate.toLocalDate
    CloudDate.fromYearMonthDay(
      localDate.getYear,
      localDate.getMonthValue,
      localDate.getDayOfMonth
    )
  }
  def convertSqlTimestampToCloudTimestamp(
      SqlTimestamp: SqlTimestamp
  ): CloudTimestamp = {
    CloudTimestamp.of(SqlTimestamp)
  }

  // Function to extract BigDecimal values (with null handling)
  def convertBigDecimalToSpannerFloat(
      row: Row,
      columnName: String
  ): Option[Double] = {
    Option(row.getAs[java.math.BigDecimal](columnName)).map(_.doubleValue())
  }

  // Generic function to extract values (with null handling)
  def getValue[T](row: Row, columnName: String)(implicit
      evidence: Manifest[T]
  ): Option[T] = {
    Option(row.getAs[T](columnName))
  }

// Function to build mutation dynamically
  def buildMutation(
      row: Row,
      schema: org.apache.spark.sql.types.StructType
  ): Mutation = {
    val builder = Mutation.newInsertOrUpdateBuilder("yellow_trips_enhanced_1")

    schema.fields.foreach { field =>
      val columnName = field.name
      val columnType = field.dataType

      val spannerValue: Value =
        columnType match { // Specify Value as the return type
          case org.apache.spark.sql.types.StringType =>
            Value.string(getValue[String](row, columnName).getOrElse(""))
          case org.apache.spark.sql.types.LongType =>
            Value.int64(getValue[Long](row, columnName).getOrElse(0L))
          case org.apache.spark.sql.types.TimestampType =>
            Value.timestamp(
              convertSqlTimestampToCloudTimestamp(
                row.getAs[SqlTimestamp](columnName)
              )
            )
          case org.apache.spark.sql.types.DateType =>
            Value.date(
              convertSqlDateToCloudDate(row.getAs[SqlDate](columnName))
            )
          case org.apache.spark.sql.types.DecimalType() =>
            Value.float64(
              convertBigDecimalToSpannerFloat(row, columnName).getOrElse(0.0)
            )
          // Add other data type conversions as needed
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported data type: ${columnType}"
            )
        }

      builder.set(columnName).to(spannerValue) // Use the Spanner Value
    }

    builder.build()
  }
}
