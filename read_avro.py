from fastavro import reader

# Чтение данных из Avro-файла
with open("test_products.avro", "rb") as f:
    products = list(reader(f))

# Чтение данных из Avro-файла
with open("test_purchases.avro", "rb") as f:
    purchases = list(reader(f))

# Выводим загруженные данные
print("Loaded products:")
for purchase in purchases:
    print(purchase)

# Фильтрация продуктов с ценой выше 60
filtered_products = [p for p in products if p["price"] > 60]

print("Filtered products (price > 60):")
for product in filtered_products:
    print(product)

