from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_glue_alpha as glue,
)
from constructs import Construct
from aws_cdk import (
    Stack,
    aws_iam as iam,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_glue_alpha import Job, JobExecutable, GlueVersion, PythonVersion, Code
from constructs import Construct
class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ]
        )

        glue_job = glue.Job(self, "BookStatusETLJob",
            job_name="book_status_etl",
            role=glue_role,
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V3_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_bucket(
                    bucket=Bucket.from_bucket_name(self, "MyBucket", "ecommerce-de-project-bucket"),
                    key="scripts/book_status_etl.py"
                )

            ),
            default_arguments={
                "--enable-continuous-cloudwatch-log": "true",
                "--job-language": "python",
                "--TempDir": "s3://ecommerce-de-project-bucket/temp/"
            },
            max_retries=1,
            description="Book Status ETL job managed by CDK"
        )