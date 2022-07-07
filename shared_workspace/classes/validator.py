from voluptuous import *
from voluptuous.humanize import *


class Validator:
    

    def validateDates(self, payload): 
        dates = [payload['last_order_ts'], payload['first_order_ts']]
        for date in dates : 
            try:
                datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError(f"Incorrect data format, should be YYYY-MM-DD")
                return { 
                         "body": ValueError(f"Incorrect data format, should be YYYY-MM-DD"),
                         "message" : f'provide a valid date format for as : %Y-%m-%d %H:%M:%S'
                }
        #print('Valid dates !')
        return 'Pass'



    def validateDateRange(self, payload): 
        last_order_ts = payload['last_order_ts'] 
        first_order_ts = payload['first_order_ts']
        last_order_ts = datetime.datetime.strptime(last_order_ts, '%Y-%m-%d %H:%M:%S')
        first_order_ts = datetime.datetime.strptime(first_order_ts, '%Y-%m-%d %H:%M:%S')
        current_ts = datetime.datetime.today()
        if first_order_ts > last_order_ts:
             raise ValueError(f"Error in dates passed, first_order_ts cannot be greater than last_order_ts")
             return { 
                     "body": ValueError(f" Error in dates passed, first_order_ts cannot be greater than last_order_ts"),
                     "message" : f'provide valid dates for first_order_ts & last_order_ts'
             }
        elif ((current_ts - last_order_ts).days < 0):
             raise ValueError(f"Error in dates passed, last_order_ts cannot be greater than current_ts")
             return { 
                     "body": ValueError(f" Error in dates passed, Error in dates passed, last_order_ts cannot be greater than current_ts"),
                     "message" : f'provide valid dates for last_order_ts'
             }
        return 'Pass'


    def validateSchema(self, event):
        #print('validation_schema')
        validation_schema = Schema({
                Required("customer_id") : All(int),
                Required("country_code") : All(str),  
                Required("last_order_ts") : All(str),
                Required("first_order_ts") : All(str),
                Required("total_orders") : All(int),
                Required("segment_name") : All(str)
        })

        try:
            evn = validate_with_humanized_errors(event, validation_schema)
            return 'Pass'
        except Exception as exception:
            raise ValueError(f"provide a valid payload")
            return {
                "statusCode": 400,
                "body": str(exception),
                "message" : 'provide a valid payload'
            }


    def validateCountryCode(self, payload): 
        country_code = payload['country_code'] 
        valid_country_codes = ['Peru', 'Latvia', 'Australia', 'China']
        passed_country = list(filter(lambda cc: cc == country_code,valid_country_codes))

        if len(passed_country) == 0:
             raise ValueError(f"provide valid country_codes")
             return { 
                     "body": ValueError(f" Error in country_code"),
                     "message" : f'Provide valid country_codes'
             }
        return 'Pass'


    def validateSegments(self, payload): 
        country_code = payload['segment_name'] 
        valid_country_codes = ['frequent_segment', 'recency_segment']
        passed_country = list(filter(lambda cc: cc == country_code,valid_country_codes))

        if len(passed_country) == 0:
             raise ValueError(f"Provide valid customer segment")
             return { 
                     "body": ValueError(f" Error in segment name"),
                     "message" : f'Provide valid customer segment'
             }
        return 'Pass'