import unittest
import datetime
import query1
# from airflow.models import TaskInstance, Variable
# from custom_operators.presto_operators import TwilioPrestoDBOperator
# from airflow import DAG

class Testing(unittest.TestCase):
    def test_print_data(self):
      
        actual_result = query1.print_data()
        expected_result = [[0]]
        print(actual_result)
        self.assertEqual(actual_result,expected_result)

if __name__ == '__main__':
    unittest.main()
