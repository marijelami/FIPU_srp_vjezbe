from pyspark.sql.functions import col, trim, initcap, lit, rand, when, row_number, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_retailer_dim(mysql_retailer_df, csv_retailer_df=None):
    spark = get_spark_session()

    # --- Step 1: Normalize MySQL data (osnova tvoje tablice) ---
    base_df = (
        mysql_retailer_df
        .select(
            col("id").cast("long").alias("retailer_id"),
            initcap(trim(col("name"))).alias("name"),
            col("speciality_store").cast("int")
        )
    )

    # --- Step 2: Normalize CSV data (ako postoji) ---
    if csv_retailer_df:
        csv_df = (
            csv_retailer_df
            .selectExpr("retailer_type as name")
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("retailer_id", lit(None).cast("long"))
            # Ovdje dodajemo random logiku za nove trgovce iz CSV-a
            .withColumn("speciality_store", when(rand() < 0.4, lit(1)).otherwise(lit(0)))
            .dropDuplicates(["name"])
        )

        # --- Step 3: UNION (Spajanje baze i CSV-a) ---
        # unionByName spaja bazu i nove zapise iz CSV-a
        combined_df = base_df.unionByName(csv_df)
    else:
        # Ako nema CSV-a, koristi samo podatke iz baze
        combined_df = base_df

    # --- Step 4: Finalno čišćenje i Surrogate Key ---
    # dropDuplicates po imenu osigurava da ne dupliramo trgovce koji su u oba izvora
    retailer_df = combined_df.dropDuplicates(["name"])

    window = Window.orderBy("name")
    final_df = (
        retailer_df
        .withColumn("retailer_tk", row_number().over(window))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit(None).cast("timestamp"))
        .select("retailer_tk", "version", "date_from", "date_to", "retailer_id", "name", "speciality_store")
    )

    # assert mora odgovarati broju zapisa u bazi + novim zapisima iz CSV-a
    assert final_df.count() >= 8 
    return final_df