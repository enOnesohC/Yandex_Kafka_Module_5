# Инструкция по проверке

## Описание проекта

Данный проект показывает работу с сертификатами и правами доступа kafka. Окружение проекта состоит из контейнеров docker:

- kafka: кластер kafka из трёх брокеров 
- ui kafka: интерфейс для взаимодействия с брокерами kafka

- Main.py - файл с запуском двух продьюсеров и двух консьюмеров в потоках
- ProducerObject.py - файл с классом продьюсера, содержащий настройки для аутентификации 
- ConsumerObject.py - файл с классом консьюмера, содержащий настройки для аутентификации  

## Шаг 1.
Проверьте, установлен ли Docker на вашем компьютере. Если нет, скачайте и установите его с официального сайта Docker.
Проверьте, что Docker работает, выполнив команду в терминале:

docker --version 
Ожидаемый вывод: версия установленного Docker.

## Шаг 2.

В терминале перейдите в папку, в которой будет располагаться тестовый проект
Выполнить команду:
git clone https://github.com/enOnesohC/Yandex_Kafka_Module_4.git

Ожидаемый результат: устанавливается тестовое окружение в папку

## Шаг 3.

Откройте терминал в папке с файлом docker-compose.yaml

Для запуска всех сервисов используйте команду:
docker-compose up 

## Шаг 4.

Необходимо создать топики: topic-1 и topic-2. 
Можно сделать это через UI или с помощью команд

docker exec -it module_5-kafka-0-1 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic topic-1 --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
docker exec -it module_5-kafka-0-1 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic topic-2 --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2

## Шаг 5.

Необходимо добавить пользователя через ACL используя терминал брокера.
Для этого необходимо зайти в терминал брокера:

docker exec -it module_5-kafka-0-1 bash  

Далее необходимо выполнить команду:

/opt/bitnami/kafka/bin/kafka-acls.sh 
--bootstrap-server kafka-0:9094  
--add   
--allow-principal User:admin  
--operation write
--topic topic-1

## Шаг 6.

После этого необходимо запустить файл Main.py

В консоли будет отображаться запись сообщений в топики: topic-1 и topic-2 и чтение сообщений только из топика topic-1