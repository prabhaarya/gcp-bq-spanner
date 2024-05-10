# BigQuery to Spanner using Mutations

## Why create this repo?
* When we need to move data from BigQuery to Spanner using Apache Spark in Scala
* Spanner has a limitation on mutations per transaction, particularly affecting its efficiency in handling UPSERTS using [Mutations](https://cloud.google.com/spanner/docs/dml-versus-mutations#mutations-concept).


The repository contains Spark Scala code crafted to retrieve data from BigQuery and then transfer it to Spanner through Mutations. These operations encompass both Inserts and Updates.

## Walkthrough of the code:
* Mutation Building: The buildMutation function constructs a single Mutation object for each row of data extracted from the BigQuery table.
* Mutation Batching: The code utilizes an ArrayBuffer to accumulate mutations.
* Batch Size: The if (mutations.size >= 100) condition triggers writing the accumulated mutations to Spanner once the buffer reaches a size of 100.
* Final Write: Any remaining mutations in the buffer are written after processing all rows in the partition.
* Transactions per Mutation: The code effectively batches 100 mutations into a single transaction when writing to Spanner.


