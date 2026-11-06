"""Build a Bronze, Silver and Gold fulfillment lakehouse from synthetic CDC events."""
import argparse
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window, functions as F

FIELDS = ['order_id', 'version', 'warehouse', 'promised_date', 'delivered_date', 'status']


def spark_session():
    return configure_spark_with_delta_pip(
        SparkSession.builder.master('local[2]').appName('Fulfillment Lakehouse')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.databricks.delta.snapshotPartitions', '2')
    ).getOrCreate()


def build(spark, source, output):
    """Rebuild Bronze from the full event history, then merge only newer Silver versions."""
    output = Path(output).resolve()
    raw = spark.read.text(str(Path(source).resolve())).withColumnRenamed('value', 'raw_json')
    raw.write.format('delta').mode('overwrite').save(str(output / 'bronze'))
    schema = ', '.join(f'{name} STRING' for name in FIELDS)
    parsed = raw.withColumn('event', F.from_json('raw_json', schema)).select('raw_json', 'event.*')
    typed = (parsed.withColumn('version_number', F.expr('try_cast(version as BIGINT)'))
             .withColumn('promise', F.expr('try_cast(promised_date as DATE)'))
             .withColumn('delivery', F.expr('try_cast(delivered_date as DATE)')))
    qualified = typed.withColumn('reason',
        F.when(F.col('order_id').isNull() | ~F.col('order_id').rlike(r'^[A-Z0-9-]+$'), 'invalid_order_id')
         .when(F.col('version_number').isNull() | (F.col('version_number') < 1), 'invalid_version')
         .when(F.col('warehouse').isNull() | ~F.col('warehouse').rlike(r'^[A-Z0-9-]+$'), 'invalid_warehouse')
         .when(F.col('promise').isNull() | ~F.col('promised_date').rlike(r'^\d{4}-\d{2}-\d{2}$'), 'invalid_promise_date')
         .when(F.col('status').isNull() | ~F.col('status').isin('open', 'delivered', 'cancelled'), 'invalid_status')
         .when((F.col('status') == 'delivered') & (F.col('delivery').isNull() | ~F.col('delivered_date').rlike(r'^\d{4}-\d{2}-\d{2}$')), 'invalid_delivery_date')
         .when((F.col('status') != 'delivered') & F.col('delivered_date').isNotNull(), 'unexpected_delivery_date'))
    qualified.where(F.col('reason').isNotNull()).select('raw_json', 'reason').write.format('delta').mode('overwrite').save(str(output / 'quarantine'))
    valid = qualified.where(F.col('reason').isNull()).select('order_id', 'warehouse', 'status',
        F.col('version_number').alias('version'), F.col('promise').alias('promised_date'),
        F.col('delivery').alias('delivered_date')).distinct()
    if valid.groupBy('order_id', 'version').count().where('count > 1').take(1):
        raise ValueError('Conflicting payloads share an order ID and version; correct the input history')
    latest = (valid.withColumn('rank', F.row_number().over(Window.partitionBy('order_id').orderBy(F.desc('version'))))
              .where('rank = 1').drop('rank'))
    silver_path = str(output / 'silver')
    if DeltaTable.isDeltaTable(spark, silver_path):
        table = DeltaTable.forPath(spark, silver_path)
        different = ' OR '.join(f'NOT (s.{col} <=> t.{col})' for col in ('warehouse', 'status', 'promised_date', 'delivered_date'))
        if valid.alias('s').join(table.toDF().alias('t'), F.expr('s.order_id = t.order_id AND s.version = t.version')).where(different).take(1):
            raise ValueError('Input conflicts with a version already stored in Silver')
        (table.alias('t').merge(latest.alias('s'), 't.order_id = s.order_id')
         .whenMatchedUpdateAll(condition='s.version > t.version').whenNotMatchedInsertAll().execute())
    else:
        latest.write.format('delta').save(silver_path)
    spark.read.format('delta').load(silver_path).createOrReplaceTempView('current_orders')
    gold = spark.sql('''
        SELECT warehouse, promised_date,
               COUNT(*) AS orders,
               SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
               SUM(CASE WHEN status = 'delivered' AND delivered_date <= promised_date THEN 1 ELSE 0 END) AS on_time,
               SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
        FROM current_orders GROUP BY warehouse, promised_date
    ''').withColumn('on_time_pct', F.when(F.col('delivered') > 0, F.round(100 * F.col('on_time') / F.col('delivered'), 2)))
    gold.write.format('delta').mode('overwrite').save(str(output / 'gold'))
    return gold


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', default='examples/orders.jsonl')
    parser.add_argument('--output', default='artifacts')
    args = parser.parse_args()
    spark = spark_session()
    try:
        build(spark, args.input, args.output).orderBy('warehouse', 'promised_date').show(truncate=False)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
