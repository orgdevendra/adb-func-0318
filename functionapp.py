import azure.functions as func
import logging
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.webpubsub import WebPubSubServiceClient
from azure.messaging.webpubsubservice import WebPubSubServiceClient

# Initialize Service Bus client
# Azure Service Bus connection details
SERVICE_BUS_CONNECTION_STR = os.getenv('SERVICE_BUS_CONNECTION_STR')
WEATHER_TOPIC = 'weather-topic'
WIND_TOPIC = 'wind-topic'

# Azure Web PubSub connection details
WEBPUBSUB_CONNECTION_STR = os.getenv('WEBPUBSUB_CONNECTION_STR')
HUB_NAME = 'notificationhub'

# Initialize Service Bus client
servicebus_client = ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR)

# Initialize Web PubSub client
webpubsub_client = WebPubSubServiceClient.from_connection_string(WEBPUBSUB_CONNECTION_STR, hub=HUB_NAME)

def process_message(message):
    # Send message to Web PubSub
    webpubsub_client.send_to_all(message)

def listen_to_topic(topic_name):
    with servicebus_client.get_subscription_receiver(topic_name=topic_name, subscription_name='mysubscription') as receiver:
        for msg in receiver:
            print(f"Received message from {topic_name}: {msg}")
            process_message(str(msg))
            receiver.complete_message(msg)

if __name__ == "__main__":
    listen_to_topic(WEATHER_TOPIC)
    listen_to_topic(WIND_TOPIC)

