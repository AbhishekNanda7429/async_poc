#testcase1

import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Initialize the SQS client
    sqs_client = boto3.client('sqs')

    # URL of the existing SQS queue
    queue_url = "https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue"

    # Example data to send
    messages = [{'Id': str(i), 'MessageBody': f' adad addad adq3e23e3r rrwrqw qwerqw vsgegeg wgertertetrgeggerg erwerwrwr erwrwr q3e2e2 wefwe werwrwr wrwerwerwer wewrwrwr rwrqrqrwrwwwrwrwerwe werwrwrwrwrwr werwrwerwerwer werwewerwerwer werwerwrwewer  {i}'} for i in range(1, 12001)]

    # Function to send messages in batches
    def send_batch(batch):
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=batch
            )
            print(f"Successfully sent messages: {response}")
        except ClientError as e:
            print(f"An error occurred: {e}")

    # Break the messages into batches of 10
    batch_size = 10
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        send_batch(batch)

    return {
        'statusCode': 200,
        'body': json.dumps('Messages sent successfully in batches!')
    }
#-------------------------------------------------------------------------

#testcase2

# import json
# import boto3
# from botocore.exceptions import ClientError

# def lambda_handler(event, context):
#     # Initialize the SQS client
#     sqs_client = boto3.client('sqs')

#     # URL of the existing SQS queue
#     queue_url = "https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue"

#     # Example data to send
#     messages = [{'Id': str(i), 'MessageBody': f'Message {i}'} for i in range(1, 101)]

#     # Split messages into batches (batch_size can be adjusted as needed)
#     batch_size = 10
#     message_batches = [messages[i:i+batch_size] for i in range(0, len(messages), batch_size)]

#     try:
#         # Send messages in batches
#         for batch in message_batches:
#             entries = [{'Id': message['Id'], 'MessageBody': message['MessageBody']} for message in batch]
#             response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)
#             print(f"Successfully sent messages: {response['Successful']}")
#     except ClientError as e:
#         print(f"An error occurred: {e}")

#     return {
#         'statusCode': 200,
#         'body': json.dumps('Messages sent successfully!')
#     }
