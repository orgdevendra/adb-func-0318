import os
import logging
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.webpubsub import WebPubSubServiceClient
import azure.functions as func

# Azure Service Bus connection details
SERVICE_BUS_CONNECTION_STR = os.getenv('SERVICE_BUS_CONNECTION_STR')
WEATHER_TOPIC = 'weather-topic'
WIND_TOPIC = 'wind-topic'

# Azure Web PubSub connection details
WEBPUBSUB_CONNECTION_STR = os.getenv('WEBPUBSUB_CONNECTION_STR')
HUB_NAME = 'notificationhub'

# Initialize Web PubSub client
webpubsub_client = WebPubSubServiceClient.from_connection_string(WEBPUBSUB_CONNECTION_STR, hub=HUB_NAME)

def main(msg: func.ServiceBusMessage):
    logging.info(f'Received message: {msg.get_body().decode("utf-8")}')
    # Send message to Web PubSub
    webpubsub_client.send_to_all(msg.get_body().decode("utf-8"))
