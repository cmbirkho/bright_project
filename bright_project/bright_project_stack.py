from aws_cdk import (
    Stack,
    aws_iam,
    aws_s3,
    RemovalPolicy,
    aws_s3_deployment,
    aws_glue,
    aws_lambda,
    CfnOutput,
    aws_events,
    aws_events_targets,
    aws_athena,
    Duration

)
from constructs import Construct
import os

class BrightProjectStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        """
        Stack for processing pbright data. 
        Includes: 
            - IAM: 
                - pbright-role which is used for all assets in the pipeline
            - S3: 
            - Glue Crawler: 
            - Lambda:
            - Athena:
            - EventBridge:
        """

        # IAM
        # Create role (using this role for everything for sake of speed)
        # Add policies to the role
        pbright_role = aws_iam.Role(self, "pbright-role",
            assumed_by=aws_iam.CompositePrincipal(
                aws_iam.ServicePrincipal("ec2.amazonaws.com"),
                aws_iam.ServicePrincipal("glue.amazonaws.com"),
                aws_iam.ServicePrincipal("lambda.amazonaws.com"),
                aws_iam.ServicePrincipal("athena.amazonaws.com")
            )
        )
        
        pbright_role.add_managed_policy(aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))
        pbright_role.add_managed_policy(aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        pbright_role.add_managed_policy(aws_iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"))
        pbright_role.add_managed_policy(aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"))

        # S3
        # Create S3 bucket for the raw data to land
        bucket_raw_data =aws_s3.Bucket(self, "pbrightBucketRawData",
                bucket_name="pbright-raw-data",
                versioned=True,
                removal_policy=RemovalPolicy.DESTROY,
                auto_delete_objects=True)
        
        # Upload the files of example data shared for the project to pbright-raw-data
        aws_s3_deployment.BucketDeployment(self, "DeployFiles",
            destination_bucket=bucket_raw_data,
            sources=[aws_s3_deployment.Source.asset("./bucket-contents")],
            exclude=["*.DS_Store"]
        )

        # Athena
        # S3 Bucket for Athena query results
        athena_query_results_bucket = aws_s3.Bucket(self, "pbrightAthenaQueryResultsBucket",
            bucket_name="pbright-athena-query-results-bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        # create workgroup
        athena_workgroup = aws_athena.CfnWorkGroup(self, "pbrightAthenaWorkgroup",
            name="pbrightWorkgroup",
            state="ENABLED",
            work_group_configuration=aws_athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=aws_athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_query_results_bucket.bucket_name}/"
                ),
                enforce_work_group_configuration=True
            )
        )

        # Glue
        # Create Glue Database
        glue_database = aws_glue.CfnDatabase(self, "pbrightDatabase",
            catalog_id=self.account,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name="pbright"
            )
        )

        # Create Glue Crawler for S3 bucket pbright-raw-data and schedule the crawler
        crawler = aws_glue.CfnCrawler(self, "pbrightCrawler",
            role = pbright_role.role_arn,
            database_name = glue_database.database_input.name,
            targets={
                "s3Targets": [{
                    "path": f"s3://{bucket_raw_data.bucket_name}/"
                }]
            },
            table_prefix="raw_",
            description="pbright raw data crawler",
            schedule={
                "scheduleExpression": "cron(0 12 * * ? *)"
            },
            classifiers=[],
            schema_change_policy={
                "updateBehavior": "UPDATE_IN_DATABASE",
                "deleteBehavior": "LOG"
            },
            configuration='''{
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior": "InheritFromTable"
                    }
                }
            }'''
        )

        # Lambda
        # Create lambda function to run queries in Athena 
        athena_lambda_function = aws_lambda.Function(self, "AthenaQueryLambda",
                                           runtime=aws_lambda.Runtime.PYTHON_3_8,
                                           handler="athena_run_handler.lambda_handler",
                                           code=aws_lambda.Code.from_asset("lambda-athena-run"),
                                           role=pbright_role,
                                           environment={
                                               'RESULT_BUCKET': athena_query_results_bucket.bucket_name,
                                               'ATHENA_WORKGROUP': athena_workgroup.name
                                           },
                                           memory_size=128,
                                           timeout=Duration.minutes(15))


        # EventBridge
        # Create EventBridge rule to schedule the Lambda function
        schedule_rule = aws_events.Rule(self, "ScheduleRule",
            schedule = aws_events.Schedule.cron(
                minute="0",
                hour="12",  # This will run the Lambda function daily at 12:00 PM UTC
                month="*",
                week_day="*",
                year="*"
            )
        )
        schedule_rule.add_target(aws_events_targets.LambdaFunction(athena_lambda_function))


        # Output the S3 bucket name and Lambda function name
        # CfnOutput(self, "pbrightBucket", value=bucket.bucket_name)
        # CfnOutput(self, "TriggerDBTRunLambda", value=dbt_lambda_function.function_name)

