import asyncio
import aioboto3

async def send_to_sqs(queue_url, message_body):
    
    session = aioboto3.Session()
    async with session.resource('sqs') as sqs:
        queue = await sqs.get_queue_by_name(QueueName=queue_url)
        await queue.send_message(MessageBody=message_body)
        print(f"Message sent to {queue_url}: {message_body}")

async def main():
    # List of messages to send
    messages = ["Hello, world!", "This is another message.", "And one more message."]
    queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
    
    # Create a list of tasks
    tasks = [send_to_sqs(queue_url, message) for message in messages]
    
    # Run the tasks concurrently
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())