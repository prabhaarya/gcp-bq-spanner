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
* **Transactions per Mutation**: The code effectively batches # of mutations calculated above into a single transaction when writing to Spanner.
* **Accumulator**: The spark.sparkContext.longAccumulator("Total Mutations") creates a long accumulator named "Total Mutations".
* **Increment Accumulator**: Inside the foreach loop where you build mutations, totalMutations.add(1) increments the accumulator for each row processed.
* **Global Batch Size Check**: The if (totalMutations.value >= batchSize) condition now checks the accumulator value. This provides a global count of mutations across all workers.
* **Reset Accumulator**: After writing a batch to Spanner, totalMutations.reset() sets the accumulator back to 0 for the next batch.
* **Final Write**: Any remaining mutations in the buffer are written after processing all rows in the partition.

## Steps to run this code:
### Notes - Build jar locally using `sbt` and then upload to Cloud Storage

* Need java 11 --> Dataproc 2.2 images all use Java 11

* SCALA (v2.12.18) (if not installed already on local)
    ```shell
    cs install scala:2.12.18 scalac:2.12.18
    ```

* build a JAR

    ```shell
    sbt package
    ```
* jar will be created in default location under target folder of the    project. For eg:
    ```
    test-lbg_2.12-0.1.0-SNAPSHOT.jar
    ```
### Notes - Run jar on Dataproc cluster on Google Cloud

* create some environment variables
    ```shell
    export PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")
    export PROJECT_NUM=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
    export GEO_REGION="europe-west2"
    export GCS_BUCKET="gs://test-lbg-cloudera/"
    export GCS_BUCKET_JARS="${GCS_BUCKET}/jars"
    export BQ_DATASET="lbg"
    export BQ_TABLE="test"
    export SPANNER_INSTANCE="test-instance"
    export SPANNER_DB="example-db"
    export SPANNER_TABLE="test"
    export CLUSTER_NAME="lbg-cluster"
    export APP_JAR_NAME="test-lbg_2.12-0.1.0-SNAPSHOT.jar"
    ```

* Upload required JARs to Google Cloud Storage bucket

    * google-cloud-spanner-jdbc-2.17.1-single-jar-with-dependencies.jar
    * google-cloud-spanner-6.45.1.jar
    * test-lbg_2.12-0.1.0-SNAPSHOT.jar --> Scala code build using sbt

* Launch Scala Apache Spark job on Dataproc cluster (assuming there is cluster available in GCP project)

    ```shell
    gcloud dataproc jobs submit spark --cluster ${CLUSTER_NAME} \
        --region=us-central1 \
        --jar=${GCS_BUCKET_JARS}/${APP_JAR_NAME} \
        --jars=${GCS_BUCKET_JARS}/google-cloud-spanner-6.45.1.jar,gs://test-lbg-cloudera/jars/google-cloud-spanner-jdbc-2.17.1-single-jar-with-dependencies.jar
    ```
### Notes - getting dev environment to match dataproc image

Need to match dev environment with environment created by dataproc image

| attribute          | Dataproc                                                                                        | Local Dev                        |
|--------------------|-------------------------------------------------------------------------------------------------|----------------------------------|
| Dataproc image     | [2.2-debian12](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2) | n/a                              |
| Apache Spark       | 3.5.0                                                                                           | n/a                              |
| BigQuery connector | 0.34.0                                                                                          | n/a                              |
| GCS connector      | 3.0.0                                                                                           | n/a                              |
| Java               | 11                                                                                              | zulu-11 (java version "11.0.20") |
| Scala              | 2.12.18                                                                                         | 2.12.18                          |
| IDE                | n/a                                                                                             | IntelliJ IDEA (2022.3.3)         |
| build system       | n/a                                                                                             | sbt                              |
| sbt                | n/a                                                                                             | 1.9.9                            |
