from dataclasses import dataclass
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def get_reason(df):
    control_column = [column for column in df.columns if column.startswith("_") and column != "_sk"]
    filter_statement = " or ".join(column for column in control_column)
    data_column = [column for column in df.columns if column not in control_column and column != "_sk"]

    df = ( df.filter(filter_statement)
          .melt(
              ids = ["_sk", *data_column],
              values = control_column,
              variableColumnName = "reason",
              valueColumnName = "status"
          ).filter(col("status")== True)
          .groupBy(col("_sk"), *data_column)
          .agg(collect_list("reason").alias("reason"))
          )
    return df

# silver layer tools

@dataclass
class silverLayer:
    table_name: str
    file_path: str
    keys: list
    schema_details: dict

    def __post_init__(self):
        self.data_column = list(self.schema_details.keys())
        self.silver_path = f"project_cat.silver_layer.{self.table_name}"
        self.bad_data_path = f"project_cat.quarantine_layer.{self.table_name}"

    @classmethod
    def get_config(cls, table_name):
        config_table = ( spark.table("project_cat.default.config_table")
                        .filter(col("table_name")== table_name)
                        .first()
        )
        return cls(
            table_name = config_table.table_name,
            file_path = config_table.file_path,
            keys = config_table.keys,
            schema_details = config_table.schema_details
        )
    
    def get_bronze_data(self):

        df = ( spark.table(self.file_path)
              .withColumn("_sk", monotonically_increasing_id())
              .drop("_file_name", "_rescued_data","_processing_time")
              )
        return df 
    
    def drop_dup_columns(self, main_table, df1, df2):

        main_df = main_table.select("_sk", *self.data_column)

        df_1 = spark.table(f"project_cat.bronze_layer.{df1}")
        df_2 = spark.table(f"project_cat.bronze_layer.{df2}")

        intersect_wt_df1 = set(main_df.columns).intersection(set(df_1.columns))
        intersect_wt_df2 = set(main_df.columns).intersection(set(df_2.columns))

        overlap_columns =  list(intersect_wt_df1.union(intersect_wt_df2))                                                      
        key_not_drop = self.keys
        column_to_drop = [c for c in overlap_columns if c not in key_not_drop]
        column_to_drop

        droped_df = main_df.drop(*column_to_drop)

        return droped_df

    def get_key_null(self, bronze_df):
        null_statement = {f"_is_{column}_null": col(column).isNull()
                           for column in self.keys}
        
        key_null_df = ( bronze_df.withColumns(null_statement)
                       .transform(get_reason)
                       )

        return key_null_df
    
    def get_dup_data(self, bronze_df, key_null_df):

        bronze_not_null = bronze_df.join(key_null_df, ["_sk"], "left_anti")

        row_dup_window = Window.partitionBy(self.data_column).orderBy("_sk")
        key_dup_window = Window.partitionBy(self.keys)

        row_dup_df = ( bronze_not_null
                      .withColumn("rn", row_number().over(row_dup_window))
                      .filter(col("rn") > 1)
                      .drop(col("rn"))
                      .withColumn("reason", array(lit("_is_row_dup")))
                     )
        key_dup_df = (bronze_not_null
                      .withColumn("cn", count("*").over(key_dup_window))
                      .filter(col("cn") > 1)
                      .drop(col("cn"))
                      .withColumn("reason", array(lit("_is_key_dup")))
                    )
                      
        dup_df = (row_dup_df
                  .unionByName(key_dup_df)
                  )
        
        return dup_df
    
    def get_bad_data(self, key_null_df, dup_df):

        bad_data = (key_null_df
                    .unionByName(dup_df)
                    .groupby("_sk", *self.data_column)
                    .agg(flatten(collect_list("reason")).alias("reason"))
                    )
        return bad_data
    
    def get_cleaned_data(self, bronze_df, bad_data):

        cast_statement = {column: col(column).cast(column_type) for column,column_type in self.schema_details.items()}
        
        cleaned_data = (bronze_df.join(bad_data, ["_sk"], "left_anti")
                        .withColumns(cast_statement))
        
        return cleaned_data
    
    def load_into_silver(self, cleaned_data):

        if spark.catalog.tableExists(self.silver_path):
            target = DeltaTable.forName(spark, (self.silver_path))
            match_condition = " and ".join([f"t.{key} == s.{key}" for key in self.keys])

            ( target.alias("t").merge(cleaned_data.alias("s"), match_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        else:
            cleaned_data.write.mode("overwrite").saveAsTable(self.silver_path)

    def load_bad_data(self, bad_data):
        bad_data.write.mode("append").saveAsTable(self.bad_data_path)