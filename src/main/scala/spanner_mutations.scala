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



class spanner_mutations (
                          var prj: String,
                          var bqDataset: String,
                          var bqTable: String,
                          var inst: String,
                          var db: String,
                          var tbl: String
                        ) extends Serializable {

  def execute_pipeline() = {
    val spark = SparkSession
      .builder()
      .appName("spark-spanner-demo")
      .config("spark.executor.memory", "8g") // Executor memory (adjust as needed)
      .config("spark.driver.memory", "4g") // Driver memory (adjust as needed)
      .config("spark.executor.instances", "13") // Number of executor instances
      .config("spark.executor.cores", "2") // Number of cores per executor (adjust based on available resources)
      .getOrCreate()
    import spark.implicits._

    val old_df = spark.read.format("bigquery").load(s"$prj.$bqDataset.$bqTable")
    old_df.createOrReplaceTempView("old_df")
    val df = spark.sql("SELECT * FROM old_df")
    
    // Get the DataFrame schema on the driver
    val schema = df.schema
    
    // Create an accumulator to track total mutations
    val totalMutations = spark.sparkContext.longAccumulator("Total Mutations")

    // Counter for mutations in the current transaction
    var mutationsInTransaction = 0 

    val maxMutationSizeInBytes = 100 * 1024 * 1024 // 100MB
    val maxMutationsPerTransaction = 80000 // Spanner's limit, adjust if needed

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

    // First: Calculate batch size
    val batchSize = Math
      .min(
        maxMutationSizeInBytes / estimatedRowSizeInBytes, // Limit by mutation size
        maxMutationsPerTransaction
      )
      .toInt
      
    // Second: Calculate dynamic batch size based on number of columns
    val numColumns = df.schema.fields.length
    val batchSize_cal = Math.max(80000 / numColumns, 1) // Adjust divisor as needed

    // Choose best out of these two
    val spanner_batch_size = Math.min(batchSize,batchSize_cal).toInt

    df.rdd.foreachPartition { partition =>
      val client = createSpannerClient(s"$prj", s"$inst", s"$db")
      val mutations = ArrayBuffer.empty[Mutation]
      var mutationsInTransaction = 0 // Counter for mutations in the current transaction

      partition.foreach { row =>
        mutations += buildMutation(
          row,
          schema,
          s"$tbl"
        ) // Pass DataFrame to buildMutation
        totalMutations.add(1) // Increment accumulator
        mutationsInTransaction += 1 // Increment transaction-specific counter

        // Use the dynamic batch size for mutations into single transaction to write spanner
        // Check if accumulated mutations reached the batch size
        if (mutationsInTransaction >= spanner_batch_size) {
          client.write(mutations.asJava) // Pass the buffer directly
          mutations.clear()
          mutationsInTransaction = 0 // Reset transaction counter
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
      schema: org.apache.spark.sql.types.StructType, 
      tbl: String
  ): Mutation = {
    val builder = Mutation.newInsertOrUpdateBuilder(s"$tbl")

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
