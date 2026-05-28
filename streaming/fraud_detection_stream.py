from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mysql.connector
from datetime import datetime

# Create Spark Session
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema
schema = StructType([
    StructField("card_id", IntegerType()),
    StructField("member_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("postcode", IntegerType()),
    StructField("pos_id", IntegerType()),
    StructField("transaction_dt", StringType())
])

# Read Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

# Convert Kafka Value
json_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Default status
fraud_df = parsed_df.withColumn("status", lit("GENUINE"))

# Function to write into MySQL
def write_to_mysql(batch_df, batch_id):

    rows = batch_df.collect()

    conn = mysql.connector.connect(
        host="localhost",
        user="frauduser",
        password="fraud123",
        database="fraud_detection"
    )

    cursor = conn.cursor()

    for row in rows:

        status = "GENUINE"

        # =====================================================
        # RULE 1 — RAPID TRANSACTION FRAUD
        # Same card used more than 3 times in 1 minute
        # =====================================================

        rapid_sql = """
        SELECT COUNT(*)
        FROM historical_transactions
        WHERE card_id = %s
        AND transaction_time >= NOW() - INTERVAL 1 MINUTE
        """

        cursor.execute(rapid_sql, (row.card_id,))
        rapid_count = cursor.fetchone()[0]

        if rapid_count >= 8:
            status = "FRAUD"

        # =====================================================
        # RULE 2 — LOCATION FRAUD
        # Different postcode within short time
        # =====================================================

        location_sql = """
        SELECT postcode
        FROM transactions
        WHERE card_id = %s
        ORDER BY id DESC
        LIMIT 1
        """

        cursor.execute(location_sql, (row.card_id,))
        location_result = cursor.fetchone()

        if location_result:

            previous_postcode = location_result[0]

            if previous_postcode != row.postcode:

                recent_sql = """
                SELECT COUNT(*)
                FROM transactions
                WHERE card_id = %s
                AND transaction_dt >= NOW() - INTERVAL 2 MINUTE
                """

                cursor.execute(recent_sql, (row.card_id,))
                recent_count = cursor.fetchone()[0]

                if recent_count >= 2:
                    status = "FRAUD"

        # =====================================================
        # RULE 3 — SPENDING SPIKE FRAUD
        # Current amount > 5x historical average
        # =====================================================

        avg_sql = """
        SELECT AVG(amount)
        FROM transactions
        WHERE card_id = %s
        """

        cursor.execute(avg_sql, (row.card_id,))
        avg_result = cursor.fetchone()[0]

        if avg_result and avg_result > 5000:

            if row.amount > (avg_result * 10):
                status = "FRAUD"

        # =====================================================
        # RULE 4 — NIGHT-TIME FRAUD
        # High-value transactions during odd hours
        # =====================================================

        current_hour = datetime.now().hour

        if current_hour >= 1 and current_hour <= 4:
            if row.amount > 20000:
                status = "FRAUD"

        # =====================================================
        # INSERT MAIN TRANSACTION
        # =====================================================

        insert_sql = """
        INSERT INTO transactions
        (card_id, member_id, amount, postcode, pos_id, transaction_dt, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """

        values = (
            row.card_id,
            row.member_id,
            row.amount,
            row.postcode,
            row.pos_id,
            row.transaction_dt,
            status
        )

        cursor.execute(insert_sql, values)

        # =====================================================
        # STORE HISTORICAL TRANSACTION
        # =====================================================

        history_sql = """
        INSERT INTO historical_transactions (card_id)
        VALUES (%s)
        """

        cursor.execute(history_sql, (row.card_id,))

        print(
            f"Card: {row.card_id}, Amount: {row.amount}, "
            f"Postcode: {row.postcode}, Status: {status}"
        )

    conn.commit()

    cursor.close()
    conn.close()

# Stream Write
query = fraud_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

query.awaitTermination()
