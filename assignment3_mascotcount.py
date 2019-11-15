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

            # Load mastcot data from BigQuery.
mascots = spark.read.format('bigquery') \
    .option('table', 'bigquery-public-data.ncaa_basketball.mascots') \
    .load()
mascots.createOrReplaceTempView('mascots')

            # Load color data from BigQuery
colors = spark.read.format('bigquery') \
        .option('table','bigquery-public-data.ncaa_basketball.team_colors')\
        .load()

colors.createOrReplaceTempView('colors') 

                                # Perform mascot + color count.
mascots_join = spark.sql(
    'SELECT coalesce(m.tax_genus,m.non_tax_type) as type, c.color, count(c.id) as cnt ' 
    'FROM mascots m, colors c where m.id = c.id group by type, c.color order by cnt desc, type desc')
mascots_join.show()
mascots_join.printSchema()

                                    # Saving the data to BigQuery
mascots_join.write.format('bigquery') \
        .option('table', 'wordcount_dataset.mascot_team_color_count_output') \
        .save()
