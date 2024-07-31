import aws_cdk as core
import aws_cdk.assertions as assertions

from bright_project.bright_project_stack import BrightProjectStack

# example tests. To run these tests, uncomment this file along with the example
# resource in bright_project/bright_project_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = BrightProjectStack(app, "bright-project")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
