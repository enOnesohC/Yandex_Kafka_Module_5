import threading
import uuid
import os

from ProducerObject import ProducerObjectTopic
from ConsumerObject import SingleMessageConsumerFirstTopic, SingleMessageConsumerSecondTopic


producer_topic_1 = ProducerObjectTopic()
producer_topic_2 = ProducerObjectTopic()
consumer_topic_1 = SingleMessageConsumerFirstTopic()
consumer_topic_2 = SingleMessageConsumerSecondTopic()

# запуск в разных потоках from deepseek
t1 = threading.Thread(target=producer_topic_1.send_messages, args=("topic-1",))
t2 = threading.Thread(target=producer_topic_2.send_messages, args=("topic-2",))
t3 = threading.Thread(target=consumer_topic_1.set_subscribe, args=("topic-1",))
t4 = threading.Thread(target=consumer_topic_2.set_subscribe, args=("topic-2",))

t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()
