import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Initialize the SQS client
    sqs_client = boto3.client('sqs')

    # URL of the existing SQS queue
    queue_url = "https://sqs.ap-south-1.amazonaws.com/891377261145/BufferQueue"

    # Example data to send
    messages = [{'Id': str(i), 'MessageBody': f'  adad addad adq3e23e3r rrwrqw qwerqw vsgegeg wgertertetrgeggerg erwerwrwr erwrwr q3e2e2 wefwe werwrwr wrwerwerwer wewrwrwr rwrqrqrwrwwwrwrwerwe werwrwrwrwrwr werwrwerwerwer werwewerwerwer werwerwrwewer {i}'} for i in range(1, 12001)]

    try:
        # Send each message individually
        for message in messages:
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message['MessageBody']
            )
            print(f"Successfully sent message with ID {message['Id']}: {response}")
    except ClientError as e:
        print(f"An error occurred: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Messages sent successfully!')
    }
