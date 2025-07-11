from pyspark.sql import SparkSession
from src.question_2.utils import (
    create_credit_card_df,
    get_partition_count,
    repartition_df,
    coalesce_df,
    apply_mask
)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CreditCardPartitionMasking").getOrCreate()

    print("\nCreating original DataFrame...")
    df = create_credit_card_df(spark)
    df.show(truncate=False)

    print(f"\nOriginal Partition Count: {get_partition_count(df)}")

    print("\nRepartitioning to 3 partitions...")
    repartitioned_df = repartition_df(df, 3)
    print(f"Partition Count after Repartition: {get_partition_count(repartitioned_df)}")

    print("\nCoalescing to 2 partitions...")
    coalesced_df = coalesce_df(repartitioned_df, 2)
    print(f"Partition Count after Coalesce: {get_partition_count(coalesced_df)}")

    print("\nApplying Mask on Card Numbers...")
    masked_df = apply_mask(df)
    masked_df.show(truncate=False)

    spark.stop()
