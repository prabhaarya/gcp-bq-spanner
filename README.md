# BigQuery to Spanner using Mutations

## Why create this repo?
* When we need to move data from BigQuery to Spanner using Apache Spark in Scala
* Spanner has a limitation on mutations per transaction, particularly affecting its efficiency in handling UPSERTS using [Mutations](https://cloud.google.com/spanner/docs/dml-versus-mutations#mutations-concept).


The repository contains Spark Scala code crafted to retrieve data from BigQuery and then transfer it to Spanner through Mutations. These operations encompass both Inserts and Updates.

## Walkthrough of the code:
* **Mutation Building**: The buildMutation function constructs a single Mutation object for each row of data extracted from the BigQuery table.
* **Mutation Batching**: The code utilizes an ArrayBuffer to accumulate mutations.
* **Get Column Count**: It retrieves the number of columns in your DataFrame (df.schema.fields.length).
* **Calculate Batch Size**: It divides a base batch size (e.g., 100) by the number of columns. This means that as the number of columns increases, the batch size will decrease to avoid exceeding Spanner's mutation limits.
* **Minimum Batch Size**: The Math.max(..., 1) ensures that the batch size is never less than 1, preventing issues if you have a very large number of columns.
* **Final Write**: Any remaining mutations in the buffer are written after processing all rows in the partition.
* **Transactions per Mutation**: The code effectively batches 100 mutations into a single transaction when writing to Spanner.


