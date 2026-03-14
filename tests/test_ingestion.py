import pytest
from pyspark.sql import SparkSession
# Add 'col' to your imports here:
from pyspark.sql.functions import col 
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

    # Assert
    assert output_df.count() == 1
    # Now 'col' will be recognized here
    assert output_df.filter(col("user_id").isNull()).count() == 0