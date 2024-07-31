import os
import boto3
import time
from int_salesforce_leads import int_salesforce_leads_1, int_salesforce_leads_2
from fact_leads import fact_leads_1, fact_leads_2

def execute_athena_query(athena_client, query, s3_output):
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': s3_output,
        },
        WorkGroup=os.environ['ATHENA_WORKGROUP']
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for the query to complete
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        time.sleep(5)
    
    if status == 'SUCCEEDED':
        return f"Query succeeded, results are available at {s3_output}{query_execution_id}.csv"
    else:
        raise Exception(f"Query {status}")

def lambda_handler(event, context):
    athena = boto3.client('athena')
    s3_output = f"s3://{os.environ['RESULT_BUCKET']}/"
    
    queries = [
        int_salesforce_leads_1.replace('\n', ''),
        int_salesforce_leads_2.replace('\n', ''),
        fact_leads_1.replace('\n', ''),
        fact_leads_2.replace('\n', '')
    ]
    
    results = []
    for query in queries:
        try:
            result = execute_athena_query(athena, query, s3_output)
            results.append(result)
        except Exception as e:
            return {
                'statusCode': 400,
                'body': str(e)
            }
    
    return {
        'statusCode': 200,
        'body': results
    }
