Это репозиторий к проекту по Kafka Streams.
Задача - получить приложение, которое будет отправлять сообщение-алерт, если сумма денег заработанных по продукту за последнюю минуту больше 3 000.
Для генерации данных использовать файлы purchase.avsc и product.avsc

Описание работы:
1. С помощью docker-compose.yaml устанавливаем Kafka
2. У нас есть два топика - products и purchases - схема данных описана в файлах purchase.avsc и product.avsc
   products имеет поля id, name, description, price
   purchases имеет поля id, quantity, productid
3. producer.py - генерирует и отправляет данные в Kafka - каждые 2 секунды отправляет 10 записей в топик purchases
4. alert.py - приложение, которое слушает топики и проверяет для каждого productid, не превышена ли выручка за продукт за последнюю минуту
   (считаем как purchase.quantity * product.price > 3 000),
   если есть превышение - отправляет сообщение об этом.
6. generate_test - генерация тестовых данных - для проверки корректности работы приложения 
