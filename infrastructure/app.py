#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import Environment
from infrastructure.infrastructure_stack import InfrastructureStack

app = cdk.App()

# Replace this with your actual AWS account ID
env = Environment(account="968043243104", region="eu-north-1")

InfrastructureStack(app, "InfrastructureStack", env=env)

app.synth()
