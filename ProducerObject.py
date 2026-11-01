import time
import uuid
import os

from confluent_kafka import Producer, serialization


class ProducerObject(Producer):
    """
    Класс, который описывает объект,
    который отправляет сообщение в Kafka
    """
    def __init__(self):
        conf = {
            "bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'), # Адрес брокера Kafka, берётся переменная окружения контейнера докера
            "acks": "all",  # Для синхронной репликации
            "retries": 3,  # Количество попыток при сбоях
        }

        conf.update(self.get_extra_config())

        self.serializer = serialization.StringSerializer("utf_8")
        super().__init__(conf)

    def get_extra_config(self) -> dict:
        """Переопределяется в наследниках для добавления своих настроек."""
        return {}


    def send_message(self, topic, value):
        try:
            # Отправка сообщения
            self.produce(
                topic=topic,
                key=str(uuid.uuid4()),
                value=self.serializer(value),
            )
            # Очистка очереди сообщений
            self.flush()
            print(f"Отправлено сообщение {value} в топик {topic}")
        except Exception as e:
            print(f"❌ Общая ошибка подключения или выполнения: {e}")

    def send_messages(self, topic):
        while True:
            try:
                message = f"message_{str(uuid.uuid4())}"
                self.send_message(topic, message)
                time.sleep(1)  # интервал между сообщениями
            except Exception as e:
                print(f"❌ Общая ошибка подключения или выполнения: {e}")


class ProducerObjectTopic(ProducerObject):
    def get_extra_config(self) -> dict:
        return {
            "bootstrap.servers": "localhost:9095",
            "security.protocol": "SSL",
            "ssl.ca.location": "ca.crt",  # Сертификат центра сертификации
            "ssl.certificate.location": "kafka-0-creds/kafka-0.crt",  # Сертификат клиента Kafka
            "ssl.key.location": "kafka-0-creds/kafka-0.key",
            "ssl.endpoint.identification.algorithm": "none",
        }
