#sqs_retriever.py
import json
import boto3
import os
import re
import time
from aws_lambda_powertools import Logger
import asyncio
import aiohttp
import aioboto3
from datetime import datetime, timezone
from aws_xray_sdk.core import xray_recorder,patch
from aws_xray_sdk.core.async_context import AsyncContext


"""
Define two env variables for testing the code in realtime production.
SANDBOX_FLG = Y means a real production env is used as Test/Sandbox
REG_EX valye is regular expression to find the testing email address. the real email address would be ignored.
"""

sandbox_flag = os.getenv("SANDBOX_FLAG", "N")
# regex = os.getenv("REG_EX", f'VCTest\d\d\d@nfl.com')
regex = os.getenv("REG_EX", r'VCTest\d\d\d@nfl.com')
# Define the maximum number of retries
MAX_RETRIES = 4

# patch([
#     'aiobotocore',
# ], raise_errors=False)

# xray_recorder.configure(service='Test', context=AsyncContext(), sampling=False)

def disable_xray_tracing(func):
    def wrapper(*args, **kwargs):
        # Disable X-Ray tracing for this function
        xray_recorder.clear_trace_entities()
        result = func(*args, **kwargs)
        xray_recorder.begin_segment('restore')
        return result
    return wrapper

"""
This class handles fetching of contacts from ESP,
updating contacts into SQS.
"""

class SqsRetriever:

    def __init__(
        self,
        SQS_QUEUE_URL,
        CONTACTS,
        correlation_id,
        CLUB,
        ESP_NAME,
        CONTEXT_REQUEST_ID
        ):
        self.SQS_QUEUE_URL = SQS_QUEUE_URL
        self.CONTACTS = CONTACTS
        self.CLUB = CLUB
        self.ESP_NAME = ESP_NAME
        self.CONTEXT_REQUEST_ID = CONTEXT_REQUEST_ID
        # Logger
        self.logger = Logger(
            service="message_retriever",
            log_uncaught_exceptions=True,
            serialize_stacktrace=False,
            POWERTOOLS_LOG_DUPLICATION_DISABLED=True,
            POWERTOOLS_LOGGER_LOG_EVENT=True
            )
        self.correlation_id = correlation_id
        self.success_ids = []  
        self.failures = []     
        self.success_emails = []
        
        self.logger.info("Initialzing SQS Retriever")

    '''The below code is for processing contacts to SQS synchronously.'''
    def is_dummy_emails(self, email) :
        return re.match(regex, email)
   
    """ Update contacts into SQS """
    def process(self):
        try:

            # self.logger.info(f"Contacts in sqs-retriever are {self.CONTACTS}")
           
            sqs = boto3.client('sqs')

            '''
            for contact in self.CONTACTS['contacts']:
                # Send each contact as a separate message to SQS queue
                if (sandbox_flag == "N" or self.is_dummy_emails(contact['email'])) :
                    response = sqs.send_message(
                        QueueUrl=self.SQS_QUEUE_URL,
                        MessageBody=json.dumps({'contacts': [contact], 'correlation_id': self.correlation_id})
                    )
                else : 
                    self.logger.info(f'Sandbox is enabled - Ignoring this email {contact}')
                
            self.logger.info("Sqs Processor - completed")

            '''

            self.logger.info(f"No of contacts to be processed are {len(self.CONTACTS['contacts'])}")
            for contact in self.CONTACTS['contacts']:
                # Send each contact as a separate message to SQS queue
                retry_count = 0
                retry_flag = True
                contact_id = contact.get('id', '')
                self.logger.info(f"contact_id is {contact_id}")
                while retry_flag and retry_count < MAX_RETRIES:
                    try:
                        if (sandbox_flag == "N" or self.is_dummy_emails(contact['email'])):

                            response = sqs.send_message(
                                QueueUrl=self.SQS_QUEUE_URL,
                                MessageBody=json.dumps({'contacts': [contact], 'correlation_id': self.correlation_id})
                            )
                            self.success_emails.append(contact['email'])
                            if contact_id:
                                self.success_ids.append(contact_id)
                            # Break out of the loop if message is sent successfully
                        else:
                            self.logger.info(f'Sandbox is enabled - Ignoring this email ')
                            # No need to retry if sandbox is enabled or it's a dummy email
                        retry_flag = False
                        
                    except Exception as e:
                            retry_count += 1
                            self.logger.error(f"An error occurred due to : {str(e)} , retrying {retry_count} time(s)")
                            
                            time.sleep(2 ** retry_count)
                            if retry_count>=MAX_RETRIES:
                                if contact_id:
                                    self.failures.append(contact_id)
                                raise
                        
                if retry_flag:
                    # If all retries are exhausted, log an error
                    self.logger.info(f'Failed to send message after {contact} {MAX_RETRIES} retries')
                    if contact_id:
                        self.failures.append(contact_id)

            self.logger.info(f"{len(self.success_emails)} contacts pushed to SQS successfully") 
            self.logger.info(f"success_ids are {self.success_ids}")       
            
            return self.success_ids, self.failures
            
        except Exception as e:
            self.logger.error(f"Retrieving contacts and processing to SQS: {str(e)}")
            raise

    ''' The below code is for async processing of contacts to SQS'''
    @disable_xray_tracing
    async def async_is_dummy_emails(self, email):
        return re.match(regex, email)
    
    @disable_xray_tracing
    async def async_process_contact(self, sqs, contact):
        
        contact_id = contact.get('id', '')
        xray_recorder.begin_segment(f"SQS Segment for {contact_id}")
        retry_count = 0
        retry_flag = True
        time1=datetime.now()
        # self.logger.info(f"Processing contact: {contact}")
        # async with xray_recorder.in_subsegment_async(f'process_contact_{contact_id}') as segment:
        # async with xray_recorder.in_subsegment('process_contact') as subsegment:
        try:
            # async with xray_recorder.in_subsegment_async(f"SQS Subsegment for {contact_id}") as subsegment:
            while retry_flag and retry_count < MAX_RETRIES:
                try:
                    if sandbox_flag == "N" or await self.async_is_dummy_emails(contact['email']):
                        response = await sqs.send_message(
                            QueueUrl=self.SQS_QUEUE_URL,
                            MessageBody=json.dumps({'contacts': [contact], 'correlation_id': self.correlation_id})
                        )
                        time2=datetime.now()
                        time_diff = time2-time1
                        self.logger.info(f"Time taken to send message to SQS: {time_diff.total_seconds()} seconds")
                        self.success_emails.append(contact['email'])
                        if contact_id:
                            self.success_ids.append(contact_id)
                        retry_flag = False
                    else:
                        self.logger.info(f'Sandbox is enabled - Ignoring this email ')
                        retry_flag = False
                except Exception as e:
                    retry_count += 1
                    self.logger.error(f"An error occurred: {str(e)}, retrying {retry_count} time(s)")
                    await asyncio.sleep(2 ** retry_count)
                    if retry_count >= MAX_RETRIES:
                        if contact_id:
                            self.failures.append(contact_id)
                        raise

        except Exception as e:
            self.logger.error(f"Error processing contact: {str(e)}")
            raise

        # finally:
        #     # Ensure to end the segment
        #     xray_recorder.end_segment()



    @disable_xray_tracing
    async def async_process(self):
        # async with xray_recorder.in_subsegment('process_contact') as subsegment:
        try:
            
            # self.logger.info(f"Contacts in sqs-retriever are {self.CONTACTS}")
        
            session = aioboto3.Session()
            async with session.client('sqs') as sqs:
                self.logger.info(f"No of contacts to be processed are {len(self.CONTACTS['contacts'])}")
                
                tasks = [self.async_process_contact(sqs, contact) for contact in self.CONTACTS['contacts']]
                await asyncio.gather(*tasks)

                self.logger.info(f"{len(self.success_emails)} contacts pushed to SQS successfully")
                self.logger.info(f"Success IDs in sqs_retriever: {self.success_ids}")
                self.logger.info(f"Failures in sqs_retriever: {self.failures}")
                return self.success_ids, self.failures

        except Exception as e:
            self.logger.error(f"Error retrieving contacts and processing to SQS: {str(e)}")
            raise