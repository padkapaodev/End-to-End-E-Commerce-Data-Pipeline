from dataclasses import dataclass
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

@dataclass
class goldLayer:
    table_name: str

    def __post_init__(self):
        self.gold_path = f"project_cat.gold_layer.{self.table_name}"

    def gold_sales_item(self):

        spark = SparkSession.builder.getOrCreate()
        dim_products = spark.table("project_cat.silver_layer.dim_products")
        dim_customers = spark.table("project_cat.silver_layer.dim_customers")
        fact_sales = spark.table("project_cat.silver_layer.fact_sales")
        fact_orders = spark.table("project_cat.silver_layer.fact_orders")

        sales_item = ( fact_sales.alias("f1")
                      .join(dim_products.alias("d1"), ["product_id"], "left")
                      .join(fact_orders.alias("f2"), ["order_id"], "left")
                      .join(dim_customers.alias("d2"), ["customer_id"], "left")
                      .select("f1.order_id",
                               "f2.order_date_time",
                                "f2.customer_id",
                                "d2.customer_name",
                                "d2.customer_segment", 
                                "d2.region", 
                                "f1.product_id", 
                                "d1.product_name", 
                                "d1.product_category", 
                                "d1.brand", "f1.quantity", 
                                "f1.gross_sales", 
                                "f2.discount_amount", 
                                "f1.net_sales", 
                                "f1.profit", 
                                "f2.sales_channel")
                                )
        
        return sales_item
    
    def gold_product_performance(self):

        spark = SparkSession.builder.getOrCreate()
        dim_products = spark.table("project_cat.silver_layer.dim_products")
        fact_sales = spark.table("project_cat.silver_layer.fact_sales")

        product_performance  = ( fact_sales.alias("f")
                          .join(dim_products.alias("d"), ["product_id"], "left")
                          .select("f.product_id", 
                                  "d.product_name", 
                                  "d.product_category", 
                                  "d.product_subcategory", 
                                  "d.brand", "d.supplier", 
                                  "d.unit_price", 
                                  "d.product_cost", 
                                  "d.product_rating",
                                  "f.quantity", 
                                  "f.gross_sales")
                                )
        
        return product_performance
    
    def gold_customer_monthly_summary(self):

        spark = SparkSession.builder.getOrCreate()
        dim_customers = spark.table("project_cat.silver_layer.dim_customers")
        fact_sales = spark.table("project_cat.silver_layer.fact_sales")
        fact_orders = spark.table("project_cat.silver_layer.fact_orders")

        joined_df =  ( fact_sales.alias("f1")
                                .join(fact_orders.alias("f2"), ["order_id"], "left")
                                .join(dim_customers.alias("d"), ["customer_id"], "left")
                                .withColumn("order_month", date_format("f2.order_date_time", "yyyy-MM"))
                                        )

        monthly_summary = ( joined_df
                    .groupBy( col("d.customer_id"), 
                             col("d.customer_name"),
                             col("d.customer_segment"),
                             col("d.region"),
                             col("order_month"))
                    .agg( countDistinct("f1.order_id").alias("total_orders"),
                          sum("f1.quantity").alias("total_quantity"),
                          sum("f1.net_sales").alias("total_spend"),
                          sum("f1.profit").alias("total_profit"))
                    .select(
                            "customer_id",
                            "customer_name",
                            "customer_segment",
                            "region",
                            "order_month",
                            "total_orders",
                            "total_quantity",
                            "total_spend",
                            "total_profit"
                            ))
        return monthly_summary
    
    def load_into_gold(self, gold_df):
            gold_df.write.mode("overwrite").saveAsTable(self.gold_path)