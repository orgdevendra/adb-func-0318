import azure.functions as func
import logging
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.messaging.webpubsubservice import WebPubSubServiceClient

# Initialize Service Bus client
servicebus_client = ServiceBusClient.from_connection_string("Endpoint=sb://servicebus-topic-03-18.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=05RDkAWZtI6Z6EBy59py9h1PInh3BbNhN+ASbOzh8/c=")

# Initialize Web PubSub client
webpubsub_client = WebPubSubServiceClient.from_connection_string("Endpoint=https://webpub0318.webpubsub.azure.com;AccessKey=EKkUmtHah3LtgmWMG6tFQYq0O2uCizSTgxMASdLxI5UQ5gMFehYfJQQJ99BCACmepeSXJ3w3AAAAAWPS2C8P;Version=1.0;")

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        # Send message to Weather topic
        with servicebus_client.get_topic_sender(topic_name="weather_topic") as sender:
            message = ServiceBusMessage(f"Weather update: {name}")
            sender.send_messages(message)
        
        # Send message to Wind topic
        with servicebus_client.get_topic_sender(topic_name="wind_topic") as sender:
            message = ServiceBusMessage(f"Wind update: {name}")
            sender.send_messages(message)
        
        # Trigger Web PubSub push notification for Weather topic
        webpubsub_client.send_to_all("YourHubName", content=f"Weather update: {name}", content_type="text/plain")
        
        # Trigger Web PubSub push notification for Wind topic
        webpubsub_client.send_to_all("YourHubName", content=f"Wind update: {name}", content_type="text/plain")

        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
