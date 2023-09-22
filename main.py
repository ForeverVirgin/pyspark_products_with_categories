import pyspark
from products_analysis import return_products_with_categories

import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# df_products = spark.read.csv('data/products.csv', header=True, inferSchema=True)
# df_categories = spark.read.csv('data/categories.csv', header=True, inferSchema=True)
# df_matches = spark.read.csv('data/matches.csv', header=True, inferSchema=True)

data = [("1","Bread","20"),
    ("2","Butter","80"),
    ("3","Milk","70"),
    ("4","Cereal","55")
  ]
columns = ["product_id","product_name","cost"]
df_products = spark.createDataFrame(data = data, schema = columns)

data = [("1","Category 1"),
    ("2","Category 2"),
    ("3","Category 3"),
    ("4","Category 4")
  ]
columns = ["category_id","category_name"]
df_categories = spark.createDataFrame(data = data, schema = columns)

data = [("1","2"),
    ("1","3"),
    ("2","1"),
    ("3","2"),
    ("3","3")
  ]
columns = ["product_id","category_id"]
df_matches = spark.createDataFrame(data = data, schema = columns)

return_products_with_categories.execute(df_products, df_categories, df_matches).show()
