import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r"D:\GradClasses\Spring19\698\citibike-dashboard\gcp\ingest\capstone-265dde21e8f1.json")
project_id = 'capstone-231016'


subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_id,
    topic='weather',  # Set this to something appropriate.
)

subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project_id,
    sub='app-tester-1',  # Set this to something appropriate.
)

# subscriber.create_subscription(
#     name=subscription_name, topic=topic_name)

def callback(message):
    print(message.data)
    message.ack()

while True:
    import time
    future = subscriber.subscribe(subscription_name, callback)
    future.result()
    print('sleeping')
    time.sleep(5)
