from pyspark.sql.functions import explode, lit, lower, regexp_replace, split
from pyspark.sql.types import IntegerType, StringType, StructType


class VocabCreator:
    def __init__(self, path: str, spark):
        self.spark = spark
        self.path = path
        self.schema = StructType() \
            .add("sentiment", IntegerType(), True) \
            .add("text", StringType(), True)
        self.df = self.spark.read.csv(self.path, schema=self.schema)

    def create_vocabulary(self):
        data = self.df.withColumn("dummy_col", lit(1))

        data = data.withColumn("array_col", split("text", " "))

        words = data.withColumn("explode_col", explode("array_col"))

        words = words.drop("sentiment").drop("array_col").drop("text")
        words = words.groupBy("explode_col").sum("dummy_col")
        words = words.select(
            "explode_col",
            "sum(dummy_col)",
            (
                lower(regexp_replace("explode_col", "[^a-zA-Z\\s]", "")).alias(
                    "text"
                )
            ),
        )
        words = words.drop("explode_col")
        words = words.filter(words["sum(dummy_col)"] > 20).orderBy(
            "sum(dummy_col)"
        )

        words.repartition(1).write.format("com.databricks.spark.csv").save(
            "vocab.csv"
        )

    def clean_text(self):
        clean_data = self.df.select(lower(regexp_replace("text", "[^a-zA-Z\\s]", "")).alias(
            "clean_text"
        ))

        clean_data.repartition(1).write.format("com.databricks.spark.csv").save(
            "clean_data.csv"
        )
