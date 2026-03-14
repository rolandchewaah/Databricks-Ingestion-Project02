import pytest
from pyspark.sql import SparkSession
from src.ingestion_job import apply_silver_transformations
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_silver_transformation_filters_nulls(spark):
    # Arrange
    source_df = spark.createDataFrame(
        [("user_1", "login"), (None, "click"), ("user_1", "login")], 
        ["user_id", "event"]
    )
    
    # Act
    output_df = apply_silver_transformations(source_df)

    # Assert (Should have 1 row: user_1)
    assert output_df.count() == 1
    assert output_df.filter(col("user_id").isNull()).count() == 0