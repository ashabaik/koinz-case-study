from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'postgres': {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'koinz'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password'),
        'table': 'app_user_visits_fact'
    },
    'clickhouse': {
        'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
        'port': os.getenv('CLICKHOUSE_PORT', '8123'),
        'database': os.getenv('CLICKHOUSE_DB', 'default'),
        'table': 'app_user_visits_fact',
        'user': os.getenv('CLICKHOUSE_USER', 'default'),
        'password': os.getenv('CLICKHOUSE_PASSWORD', '')
    },
    'checkpoint': {
        'path': os.getenv('CHECKPOINT_PATH', './checkpoint/last_timestamp.txt')
    }
}

def create_spark_session():
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("Koinz-PostgreSQL-to-ClickHouse-ETL") \
        .config("spark.jars", "postgresql-42.6.0.jar,clickhouse-jdbc-0.4.6-all.jar") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    logger.info(f"Spark session created. Version: {spark.version}")
    return spark

def get_last_checkpoint(checkpoint_path):
    checkpoint_file = Path(checkpoint_path)
    
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                timestamp = int(f.read().strip())
            logger.info(f"Loaded checkpoint: {timestamp}")
            return timestamp
        except Exception as e:
            logger.error(f"Error reading checkpoint: {e}")
            return 0
    else:
        logger.info("No checkpoint found. First run.")
        return 0

def save_checkpoint(checkpoint_path, timestamp):
    try:
        checkpoint_file = Path(checkpoint_path)
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(checkpoint_file, 'w') as f:
            f.write(str(timestamp))
        
        logger.info(f"Checkpoint saved: {timestamp}")
        
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")
        raise

def read_from_postgres(spark, config, last_timestamp):
    logger.info("Reading from PostgreSQL...")
    logger.info(f"Filter: updated_at > {last_timestamp}")
    
    jdbc_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
    
    query = f"""
        (SELECT * FROM {config['table']} 
         WHERE updated_at > {last_timestamp}
         ORDER BY updated_at) AS incremental_data
    """
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "10000") \
            .load()
        
        df.cache()
        record_count = df.count()
        
        if record_count > 0:
            logger.info(f"Read {record_count} records from PostgreSQL")
            max_updated_at = df.agg(spark_max("updated_at")).collect()[0][0]
            logger.info(f"Latest updated_at: {max_updated_at}")
        else:
            logger.info("No new records found")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading from PostgreSQL: {e}")
        raise

def write_to_clickhouse(df, config):
    logger.info("Writing to ClickHouse...")
    
    jdbc_url = f"jdbc:clickhouse://{config['host']}:{config['port']}/{config['database']}"
    
    try:
        record_count = df.count()
        
        if record_count == 0:
            logger.info("No records to write")
            return
        
        logger.info(f"Writing {record_count} records...")
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", config['table']) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
        
        logger.info(f"Successfully wrote {record_count} records to ClickHouse")
        
    except Exception as e:
        logger.error(f"Error writing to ClickHouse: {e}")
        raise

def run_etl():
    spark = None
    
    try:
        logger.info("=" * 80)
        logger.info("STARTING ETL JOB")
        logger.info(f"Start Time: {datetime.now()}")
        logger.info("=" * 80)
        
        spark = create_spark_session()
        
        last_timestamp = get_last_checkpoint(CONFIG['checkpoint']['path'])
        
        df = read_from_postgres(spark, CONFIG['postgres'], last_timestamp)
        
        if df.count() > 0:
            write_to_clickhouse(df, CONFIG['clickhouse'])
            
            max_updated_at = df.agg(spark_max("updated_at")).collect()[0][0]
            save_checkpoint(CONFIG['checkpoint']['path'], max_updated_at)
        else:
            logger.info("No data to process")
        
        logger.info("=" * 80)
        logger.info("ETL JOB COMPLETED SUCCESSFULLY")
        logger.info(f"End Time: {datetime.now()}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("ETL JOB FAILED")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        return False
        
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)