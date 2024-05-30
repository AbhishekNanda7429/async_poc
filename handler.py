import asyncio
import aioboto3

# Initialize the SQS client outside the handler function
sqs_client = aioboto3.client('sqs')

async def send_to_sqs(queue_url, message_body):
    """
    Sends a message to an SQS queue asynchronously.

    Args:
        queue_url (str): The URL of the SQS queue.
        message_body (str): The message body to send.
    """
    response = await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body
    )
    print(f"Message sent to {queue_url}: {message_body}")

def lambda_handler(event, context):
    """
    AWS Lambda function handler.

    Args:
        event (dict): The event data from the Lambda invocation.
        context (dict): The context data from the Lambda invocation.
    """
    queue_url = 'https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue'
    messages = event.get("messages", [])

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