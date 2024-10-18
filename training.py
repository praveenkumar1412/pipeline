#TRAINING DATA FOR GAS SERVICE

training_bill="gs://training_data26072024/training_data_gas/gas_training_data.pdf"
training_message=""""Extract the gas service information with the columns named: customer_name, account_number, address, service_start_date, service_end_date,current_reading, previous_reading, meter, total_usage and total_cost in JSON format. Ensure that the extracted information is related to gas service alone. Use the date format YYYY-MM-DD. If any value is missing for a particular column, assign null for that value. Do not add any special characters or formatting outside of the valid JSON structure.
answer:
[
    {
        "customer_name": "IA21WEST OWNER LLC",
        "account_number": "3238 0882 22",
        "address": "1701 TAFT AVE/CHEYENNE, WY",
        "service_start_date": "2023-09-19",
        "service_end_date": "2023-10-19",
        "current_reading": "5322",
        "previous_reading": "5117",
        "meter": "BHE325358",
        "total_gas_usage": "184",
        "gas_usage_unit": "Therms",
        "total_cost": "180.64"
    }
]"""