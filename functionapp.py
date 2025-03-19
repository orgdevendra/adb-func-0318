import logging
import azure.functions as func
from azure.webpubsub import WebPubSubServiceClient

def main(msg: func.ServiceBusMessage):
    logging.info('Python ServiceBus topic trigger function processed a message: %s', msg.get_body().decode('utf-8'))

    # Initialize Web PubSub client
    webpubsub_client = WebPubSubServiceClient.from_connection_string("WEBPUBSUB_CONNECTION_STR")

    # Send message to Web PubSub
    message = msg.get_body().decode('utf-8')
    webpubsub_client.send_to_all(message)
