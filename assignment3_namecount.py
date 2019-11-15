#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession

spark = SparkSession \
          .builder \
            .master('yarn') \
              .appName('spark-bigquery-demo') \
                .getOrCreate()

                # Use the Cloud Storage bucket for temporary BigQuery export data used
                # by the connector. This assumes the Cloud Storage connector for
                # Hadoop is configured.
bucket = spark.sparkContext._jsc.hadoopConfiguration().get(
                    'fs.gs.system.bucket')
spark.conf.set('temporaryGcsBucket', bucket)

                    # Load data from BigQuery.
names = spark.read.format('bigquery') \
    .option('table', 'bigquery-public-data.usa_names.usa_1910_2013') \
    .load()
names.createOrReplaceTempView('names')

                                # Perform name count.
name_count = spark.sql(
    'SELECT name, SUM(number) AS name_count FROM names GROUP BY name')
name_count.show()
name_count.printSchema()

                                    # Saving the data to BigQuery
name_count.write.format('bigquery') \
        .option('table', 'wordcount_dataset.namecount_output') \
        .save()
