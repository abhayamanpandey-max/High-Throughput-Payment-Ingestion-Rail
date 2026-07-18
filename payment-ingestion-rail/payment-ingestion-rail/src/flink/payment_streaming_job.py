import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def run_payment_analytics_job():
    # 1. Initialize core Stream Execution Context (matches task slots configured in docker)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # 2. Bind the Flink Table API framework
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # 3. Dynamic Injection of the Java Kafka SQL Connector Jar
    kafka_jar_path = "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar"
    table_env.get_config().get_configuration().set_string("pipeline.jars", kafka_jar_path)
    
    print("[+] PyFlink stream engine loaded. Declaring event streaming sources...")
    
    # 4. Define the DDL Mapping to the Kafka Transaction Stream
    # Note: WATERMARK strategy enables tracking out-of-order events within a 5-second boundary
    source_ddl = """
        CREATE TABLE kafka_payment_source (
            transaction_id STRING,
            account_id STRING,
            amount DOUBLE,
            currency STRING,
            merchant STRING,
            `ts_string` STRING,
            location STRING,
            device_ip STRING,
            ts AS TO_TIMESTAMP(REPLACE(`ts_string`, 'T', ' ')),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'payment_transactions',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-anomaly-engine',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    table_env.execute_sql(source_ddl)
    print("[+] Kafka source table mapping established successfully.")

    # 5. Define a print table sink to verify active streams locally
    sink_ddl = """
        CREATE TABLE print_sink (
            transaction_id STRING,
            account_id STRING,
            amount DOUBLE,
            merchant STRING,
            ts TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )
    """
    table_env.execute_sql(sink_ddl)

    # 6. Execute a simple continuous pipeline routing script to test processing
    print("[+] Launching live real-time stream transformation job pipeline...")
    pipeline_query = """
        INSERT INTO print_sink
        SELECT transaction_id, account_id, amount, merchant, ts 
        FROM kafka_payment_source
        WHERE amount > 1000.00
    """
    table_env.execute_sql(pipeline_query)

if __name__ == "__main__":
    run_payment_analytics_job()
