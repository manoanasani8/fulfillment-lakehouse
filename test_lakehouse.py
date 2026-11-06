import tempfile
import unittest
from pathlib import Path

from lakehouse import build, spark_session


class LakehouseIntegration(unittest.TestCase):
    def test_quality_late_updates_and_delivery_metrics(self):
        spark = spark_session()
        self.addCleanup(spark.stop)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / 'lakehouse'
            first = {row.warehouse: row for row in build(spark, 'examples/orders.jsonl', output).collect()}
            self.assertEqual((first['CHI'].orders, first['CHI'].delivered, first['CHI'].on_time_pct), (2, 2, 50.0))
            self.assertIsNone(first['PHL'].on_time_pct)
            self.assertEqual(spark.read.format('delta').load(str(output / 'bronze')).count(), 7)
            self.assertEqual([r.reason for r in spark.read.format('delta').load(str(output / 'quarantine')).collect()], ['invalid_promise_date'])
            history = root / 'history.jsonl'
            history.write_text('\n'.join(Path(f'examples/{name}.jsonl').read_text().strip()
                                         for name in ('orders', 'late-updates')) + '\n', encoding='utf-8')
            second = {row.warehouse: row for row in build(spark, history, output).collect()}
            self.assertEqual((second['CHI'].orders, second['CHI'].on_time_pct), (2, 100.0))
            silver = spark.read.format('delta').load(str(output / 'silver'))
            self.assertEqual({r.order_id: r.version for r in silver.collect()}, {'ORD-1': 2, 'ORD-2': 3, 'ORD-3': 1})


if __name__ == '__main__':
    unittest.main()
