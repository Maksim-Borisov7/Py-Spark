from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Product Category Relationship") \
    .getOrCreate()

products_data = [
    (1, "Мандарин"),
    (2, "Черешня"),
    (3, "Абрикос"),
    (4, "Банан"),
    (5, "Яблоко")
]

categories_data = [
    (1, "Фрукты"),
    (2, "Цитрусовые"),
    (1, "Сладкие"),
    (3, "Ягодные")
]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

relationships_data = [
    (1, 1),
    (1, 2),
    (3, 1)
]

relationships_df = spark.createDataFrame(relationships_data, ["product_id", "category_id"])

def get_product_category_pairs_and_no_category_products(products_df, categories_df, relationships_df):
    product_category_pairs = products_df.alias("p") \
        .join(relationships_df.alias("r"), col("p.product_id") == col("r.product_id"), "left") \
        .join(categories_df.alias("c"), col("r.category_id") == col("c.category_id"), "left") \
        .select("product_name", "category_name")

    no_category_products = products_df.alias("p") \
        .join(relationships_df.alias("r"), col("p.product_id") == col("r.product_id"), "left_anti") \
        .select("product_name")

    return product_category_pairs, no_category_products

product_category_pairs, no_category_products = get_product_category_pairs_and_no_category_products(products_df, categories_df, relationships_df)

product_category_pairs.show()

no_category_products.show()

spark.stop()