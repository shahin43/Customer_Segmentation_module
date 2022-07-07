import unittest 
import app 
from classes.validator import Validator




class TestApp(unittest.TestCase):
    
    def test_validateschema(self):
        validator = Validator() 
        test_event = {  "customer_id": 123,  \
                        "country_code": "Peru", \
                        "last_order_ts": "2018-05-03 00:00:00", \
                        "first_order_ts": "2017-05-03 00:00:00", \
                        "total_orders": 15, \
                        "segment_name": "recency_segment" \
                     }
        res = validator.validateSchema(test_event)
        self.assertEqual(res, "Pass")


    def test_validateDates(self):
        validator = Validator() 
        test_event = {  "customer_id": 123,  \
                        "country_code": "Peru", \
                        "last_order_ts": "2018-05-03 00:00:00", \
                        "first_order_ts": "2017-05-03 00:00:00", \
                        "total_orders": 15, \
                        "segment_name": "recency_segment" \
                     }
        res = validator.validateDates(test_event)
        self.assertEqual(res, "Pass")


    def test_validateDateRange(self):
        validator = Validator() 
        test_event = {  "customer_id": 123,  \
                        "country_code": "Peru", \
                        "last_order_ts": "2018-05-03 00:00:00", \
                        "first_order_ts": "2018-05-03 00:00:00", \
                        "total_orders": 15, \
                        "segment_name": "recency_segment" \
                     }
        res = validator.validateDateRange(test_event)
        self.assertEqual(res, "Pass")


    def test_validateCountryCode(self):
        validator = Validator() 
        test_event = {  "customer_id": 123,  \
                        "country_code": "Peru", \
                        "last_order_ts": "2018-05-03 00:00:00", \
                        "first_order_ts": "2018-05-03 00:00:00", \
                        "total_orders": 15, \
                        "segment_name": "recency_segment" \
                     }
        res = validator.validateCountryCode(test_event)
        self.assertEqual(res, "Pass")

    def test_validateSegments(self):
        validator = Validator() 
        test_event = {  "customer_id": 123,  \
                        "country_code": "Peru", \
                        "last_order_ts": "2018-05-03 00:00:00", \
                        "first_order_ts": "2018-05-03 00:00:00", \
                        "total_orders": 15, \
                        "segment_name": "frequent_segment" \
                     }
        res = validator.validateSegments(test_event)
        self.assertEqual(res, "Pass")



if __name__ == '__main__':
   unittest.main()