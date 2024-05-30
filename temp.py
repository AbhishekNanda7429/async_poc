# import json
# import asyncio
# import aioboto3
# import uuid
# from aws_lambda_powertools import Tracer, Logger
# from aws_lambda_powertools.utilities.typing import LambdaContext
# # from audit_package.Package import audit

# tracer = Tracer()
# logger = Logger(
#     log_uncaught_exception=True,
#     serialize_stacktrace=False,
#     POWERTOOLS_LOG_DUPLICATION_DISABLE=True,
#     POWERTOOLS_LOGGER_LOG_EVENT=True
# )

# queue_url = 'https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue'

# async def send_message_to_sqs(message_body):
#     """
#     Sends a message asynchronously to an SQS queue.

#     Args:
#         message_body (dict): The message body to send.

#     Returns:
#         None
#     """
#     session = aioboto3.Session()
#     async with session.resource('sqs') as sqs_resource:
#         queue = await sqs_resource.get_queue_by_name(QueueName=queue_url.split('/')[-1])

#         try:
#             with tracer.capture_async_call("send_message_to_sqs"):
#                 await queue.send_message(MessageBody=json.dumps(message_body))
#             logger.info("Message sent to SQS queue successfully")
#         except Exception as e:
#             logger.error(f"Failed to send message to SQS queue: {e}")

# @logger.inject_lambda_context
# @tracer.capture_lambda_handler
# async def lambda_handler(event, context):
#     correlation_id = str(uuid.uuid4())
#     logger.append_keys(correlation_id=correlation_id)

#     clb_name = "A"
#     esp_name = "nos"
#     record_fetched = 100
#     success_record = 61
#     failure_record = 39

#     # Upload JSON data to S3 using the package function
#     # success = audit.audit(clb_name, esp_name, record_fetched, success_record, failure_record, correlation_id)

#     json_data = event
#     message_body = {
#         'correlation_id': correlation_id,
#         'data': json_data
#     }

#     await send_message_to_sqs(message_body)

#     return {
#         'statusCode': 200,
#         'body': json.dumps('JSON data sent to SQS successfully')
#     }