import asyncio
import aioboto3
import boto3
from aws_lambda_powertools import Tracer
# from aws_lambda_powertools import Logger
# from aws_lambda_powertools.utilities.typing import LambdaContext

tracer = Tracer()
# logger = Logger(
#     log_uncaught_exception=True,
#     serialize_stacktrace=False,
#     POWERTOOLS_LOG_DUPLICATION_DISABLE=True,
#     POWERTOOLS_LOGGER_LOG_EVENT=True
# )

sqs = boto3.resource('sqs')
async def send_to_sqs(queue_url, message_body):
    session = aioboto3.Session()
    async with session.resource('sqs') as sqs_resource:
        queue = await sqs_resource.get_queue_by_name(QueueName='BufferQueue')
        await queue.send_message(MessageBody=message_body)
        print(f"Message sent to {queue_url}: {message_body}")

# @logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):

    queue_url = "https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue"
    messages = event.get("messages", ["welcome", " i am abhi", "i am kishna", "i am laxmi", "i am murli", "i am sudhir", "i am anil", "i am ajay", "i am akil", "i am sudha"])

    # Create an event loop
    loop = asyncio.get_event_loop()

    # Create a list of tasks
    tasks = [send_to_sqs(queue_url, message) for message in messages]

    # Run the tasks concurrently
    loop.run_until_complete(asyncio.gather(*tasks))

    return {
        'statusCode': 200,
        'body': 'Messages sent to SQS successfully.'
    }
#--------------------------------------------------------------

# import asyncio
# import aioboto3
# from aws_lambda_powertools import Tracer

# tracer = Tracer()

# async def send_to_sqs(queue_url, message_body):
#     session = aioboto3.Session()
#     async with session.resource('sqs', region_name='ap-south-1') as sqs_resource:
#         queue = await sqs_resource.get_queue_by_name(QueueName='BufferQueue')
#         await queue.send_message(MessageBody=message_body)
#         print(f"Message sent to {queue_url}: {message_body}")

# @tracer.capture_lambda_handler
# def lambda_handler(event, context):
#     queue_url = "https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue"
#     messages = event.get("messages", ["welcome", "i am abhi", "i am kishna", "i am laxmi", "i am murli", "i am sudhir", "i am anil", "i am ajay", "i am akil", "i am sudha"])

#     async def main():
#         tasks = []
#         for message in messages:
#             async with tracer.provider.in_subsegment(f"sending_message_{message}") as subsegment:
#                 subsegment.put_annotation("message", message)
#                 tasks.append(send_to_sqs(queue_url, message))

#         await asyncio.gather(*tasks)

#     # Create a new event loop for the asynchronous tasks
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(main())
#     loop.close()

#     return {
#         'statusCode': 200,
#         'body': 'Messages sent to SQS successfully.'
#     }
