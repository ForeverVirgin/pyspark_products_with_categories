import pyspark

def execute(df_products,df_categories,df_matches):
  df_result = df_products.select(df_products["product_id"],df_products["product_name"]).join(df_matches, "product_id","left").join(df_categories.select(df_categories["category_id"],df_categories["category_name"]),"category_id","left").orderBy("product_name")
  df_result = df_result.select(df_result["product_name"],df_result["category_name"])

  return df_result