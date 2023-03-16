from ingestion_framework.Includes.Utils import try_get_column_int
from pyspark.sql import Row
from typing import Optional
from dataclasses import dataclass

@dataclass
class BronzeTarget: 

    table_name:str
    database_name:str = 'default'
    streaming_ckpt:Optional[str] = None 
    trigger_interval:Optional[str] = None
    optimized_write:Optional[str] = 'True'
    optimize_batch_frequency:Optional[int] = 100

    @classmethod
    def from_row(cls, row: Row):
        optimize_batch_frequency = try_get_column_int(row, 'optimize_batch_frequency')
        return cls(table_name=row['bronze_table'],
                   database_name=row['bronze_database'],
                   streaming_ckpt = row['bronze_checkpoint'], 
                   trigger_interval=row['trigger_interval'],
                   optimized_write=row['optimized_write'],
                   optimize_batch_frequency = optimize_batch_frequency if optimize_batch_frequency is not None else 10
                  )    
    
    def create_bronze_table(self, topic_name: str = 'N/A') -> str:
                
        return f"""CREATE TABLE IF NOT EXISTS 
                        `{self.database_name}`.`{self.table_name}` 
                        (`key` BINARY, 
                         `topic` STRING, 
                         `partition` INT, 
                         `offset` BIGINT, 
                         `timestamp` TIMESTAMP, 
                         `timestampType` INT, 
                         `value` BINARY, 
                         `headers` ARRAY<STRUCT<key:STRING, value:BINARY>>,
                         `kafka_date` DATE
                         )
                        USING DELTA
                        PARTITIONED BY (`kafka_date`)
                        COMMENT 'Bronze Delta Lake table corresponding to Kafka Topic {topic_name}'
                        TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 5)
                        """
      
    def write_to_bronze(self, df, batchId, logger, spark):

        logger.info(f"batchId is {batchId}")

        df.write.format("delta").mode("append").saveAsTable(f"{self.database_name}.{self.table_name}")

        logger.info(f"Is Optimize Enabled: {self.optimized_write}")
        optimize_batch_frequency = self.optimize_batch_frequency
        logger.info(f"optimize_batch_frequency is {optimize_batch_frequency}")
        mod = batchId%optimize_batch_frequency
        logger.info(f"batch mod is {mod}")

        if mod == 0 and self.optimized_write != 'False':
            logger.info(f"OPTIMIZING TABLE {self.database_name}.{self.table_name}")
            try:
                spark.sql(f"OPTIMIZE {self.database_name}.{self.table_name}")
            except Exception as e:
                logger.error(e.with_traceback)
 