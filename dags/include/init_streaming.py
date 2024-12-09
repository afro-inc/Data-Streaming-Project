import requests
import json
import time
import hashlib
from confluent_kafka import Producer

class KafkaProducer():

    def __init__(self, api_endpoint,bootstrap_servers, client_id, PAUSE_INTERVAL, STREAMING_DURATION, topic) -> None:
        self.api_endpoint = api_endpoint 
        self.bootstrap_servers = bootstrap_servers 
        self.client_id = client_id
        self.PAUSE_INTERVAL = PAUSE_INTERVAL
        self.STREAMING_DURATION = STREAMING_DURATION
        self.topic = topic

        
        
    def get_data(self) -> dict:
        """Makes an api request to retrieve data.

        Returns:
            dict: _description_
        """
        try:

            response = requests.get(self.api_endpoint)

            # Check the HTTP status code
            if response.status_code == 200:
                return response.json()
            
            else:
                print(f"Request failed with status code: {response.status_code}")
                print("Response Content:", response.text)

                return None

        except requests.RequestException as e:
            print(f"An error occurred: {e}")


    def encrypt_password(self, password: str) -> str:
        hash_object = hashlib.sha256()
        hash_object.update(password.encode())
        
        return hash_object.hexdigest()


    def transform(self, data: dict) -> dict:
        return {
            'id': data['id'],
            'uid': data['uid'],
            'password': self.encrypt_password(data['password']),
            'first_name': data['first_name'],
            'last_name': data['last_name'],
            'username': data['username'],
            'email': data['email'],
            'avatar': data['avatar'],
            'gender': data['gender'],
            'phone_number': data['phone_number'],
            'social_insurance_number': data['social_insurance_number'],
            'date_of_birth': data['date_of_birth'],
            'employment_title': data['employment']['title'],
            'employment_key_skill': data['employment']['key_skill'],
            'address_city': data['address']['city'],
            'address_street_name': data['address']['street_name'],
            'address_street_adress': data['address']['street_address'],
            'address_zip_code': data['address']['zip_code'],
            'address_state': data['address']['state'],
            'address_country': data['address']['country'],
            'credit_card__cc_number': data['credit_card']['cc_number'],
            'subscription_plan': data['subscription']['plan'],
            'subscription_status': data['subscription']['status'],
            'subscription_payment_method': data['subscription']['payment_method'],
            'subscription_term': data['subscription']['term']
        }

    def configure_kafka(self)-> Producer:
        """Initialize configuration to kafka instance

        Args:
            bootstrap_servers (list): the server link 
            client_id (str): the client id

        Returns:
            Producer: _description_
        """
        config = {'bootstrap.servers': self.bootstrap_servers,
                'client.id': self.client_id }
        return Producer(config)


    def publish_to_kafka(self, producer: Producer, topic: str, data: dict):
        """Publishes new data to kafka topic

        Args:
            producer (Producer): the Producer instance
            topic (string): the topic name
            data (str): the retrieved data in JSON format
        """
        producer.produce(self.topic,value =  json.dumps(data).encode('utf-8'), callback=self.delivery_report)
        producer.flush()

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result.

        Args:
            err (string): the error message
            msg (string): the message
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def init_stream(self):
        producer = self.configure_kafka()

        for _ in range(self.STREAMING_DURATION // self.PAUSE_INTERVAL):
            raw_data = self.get_data()
            kafka_formatted_data = self.transform(raw_data)
            self.publish_to_kafka(producer, self.topic, kafka_formatted_data)
            time.sleep(self.PAUSE_INTERVAL)


if __name__ == "__main__":

    api_endpoint = 'https://random-data-api.com/api/users/random_user'
    bootstrap_servers = 'localhost:9092'
    client_id = 'producer_instance'
    PAUSE_INTERVAL = 2  
    STREAMING_DURATION = 10
    topic = 'new_user'

    streaming = KafkaProducer(api_endpoint,bootstrap_servers, client_id, PAUSE_INTERVAL, STREAMING_DURATION, topic)

    streaming.init_stream()