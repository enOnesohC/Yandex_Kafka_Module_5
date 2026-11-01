import os

from confluent_kafka import Consumer, serialization


class SingleMessageConsumer(Consumer):
    """
    Консьюмер, обрабатывающий сообщения последовательно
    """
    def __init__(self):
        conf = {
            "bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),  # Адрес брокера Kafka, берётся переменная окружения контейнера докера
            "group.id": f"single_consumer_{os.getenv('GROUP_SUFFIX', 'default')}",  # Уникальный идентификатор группы, берётся переменная окружения контейнера докера
            "auto.offset.reset": "earliest",  # Начало чтения с самого начала
            "enable.auto.commit": True,  # Автоматический коммит смещений
            "session.timeout.ms": 6_000,  # Время ожидания активности от консьюмера
        }
        self.deserializer = serialization.StringDeserializer('utf_8')
        conf.update(self.get_extra_config())
        super().__init__(conf)

    def get_extra_config(self) -> dict:
        """Переопределяется в наследниках для добавления своих настроек."""
        return {}

    def set_subscribe(self, topic):
        self.subscribe([topic])
        try:
            while True:
                msg = self.poll(0.1)

                if msg is None:
                    continue
                if msg.error():
                    print(f"Ошибка SingleMessageConsumer: {msg.error()}")
                    continue

                key = msg.key().decode("utf-8") if msg.key() else None
                value = self.deserializer(msg.value())
                print(
                    f"Получено сообщение из {topic}: {key=}, {value=}, "
                    f"partition={msg.partition()}, offset={msg.offset()}"
                )
        finally:
            self.close()

class SingleMessageConsumerFirstTopic(SingleMessageConsumer):
    def get_extra_config(self) -> dict:
        return {
            "bootstrap.servers": "localhost:9095",
            "security.protocol": "SSL",
            "ssl.ca.location": "ca.crt",  # Сертификат центра сертификации
            "ssl.certificate.location": "kafka-0-creds/kafka-0.crt",  # Сертификат клиента Kafka
            "ssl.key.location": "kafka-0-creds/kafka-0.key",
            "ssl.endpoint.identification.algorithm": "none",
        }

class SingleMessageConsumerSecondTopic(SingleMessageConsumer):
    def get_extra_config(self) -> dict:
        return {}
