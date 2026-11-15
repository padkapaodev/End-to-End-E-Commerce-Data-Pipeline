from dataclasses import dataclass
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

@dataclass
class bronzeLayer:
    table_name: str 
    schema_location: str
    checkpoint_location: str
    format: str = "csv"

    @classmethod
    def get_config(cls, table_name):
        config_table = ( spark.table("project_cat.default.config_table")
                        .filter(col("table_name")== table_name)
                        .first())
        
        return cls(
            table_name = config_table.table_name,
            schema_location = config_table.schema_location,
            checkpoint_location = config_table.checkpoint_location
        )

    def get_data(self):
        df = ( spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", self.format)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaLocation", self.schema_location)
            .load(f"/Volumes/project_cat/landing_zone/raw_files/{self.table_name}")
            .withColumn("_file_name", col("_metadata.file_name"))
            .withColumn("_processing_time", current_timestamp())
        )
        return df
    
    def load_into_bronze(self, df):
        ( df.writeStream
        .outputMode("append")
        .option("mergeSchema", True)
        .option("checkpointLocation", self.checkpoint_location)
        .trigger(availableNow=True)
        .toTable(f"project_cat.bronze_layer.{self.table_name}")
        )