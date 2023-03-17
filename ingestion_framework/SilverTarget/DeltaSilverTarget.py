from ingestion_framework.SilverTarget.SilverTarget import SilverTarget
from ingestion_framework.Includes.Utils import try_get_column_str, get_trigger_interval, get_max_bytes_per_trigger, split_or_null, validate_date_string
import pyspark.sql.functions as F 
from pyspark.sql.window import Window as W
from pyspark.sql import Row, DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from dataclasses import dataclass
from typing import Iterable, Optional
from delta.tables import DeltaTable
import os
    
@dataclass
class DeltaSilverTarget(SilverTarget): 
    silver_database:str
    silver_table:str
    silver_custom_merge_path: str    
    part_cols:Optional[Iterable[str]]
       
    @classmethod
    def from_row(cls, row: Row, spark: SparkSession):
        return cls(bronze_database=row['bronze_database'],
                   bronze_table=row['bronze_table'],
                   use_schema=row['use_schema'],
                   schema_json_loc=row['schema_json_loc'],
                   silver_database=row['silver_database'],
                   silver_table=row['silver_table'],
                   silver_checkpoint=os.path.join(row['silver_checkpoint'], "_delta"),
                   silver_trans_path=row['silver_transformation_json_path'],
                   part_cols=split_or_null(try_get_column_str(row, "partition_cols")),
                   silver_custom_merge_path=row['silver_custom_merge_path'],
                   trigger_interval=try_get_column_str(row, "trigger_interval"),
                   max_bytes_per_trigger=get_max_bytes_per_trigger(try_get_column_str(row, "maxBytesPerTrigger")),
                   delta_starting_timestamp_list=validate_date_string(row['delta_starting_timestamp']),
                   spark=spark
                  )
        
    def __post_init__(self):
        super().__post_init__()
        logger = self.logger
        self.target_table_name = f"`{self.silver_database}`.`{self.silver_table}`"
        self.target_table = DeltaTable.forName(self.spark, self.target_table_name)
        self.temp_table = f"global_temp.{self.silver_table}_temp_view"

        if self.silver_custom_merge_path is not None:

            with open(self.silver_custom_merge_path) as sql_file:
                merge_statement = sql_file.read()
            self.merge_statement = merge_statement.replace('@silver_table',f"{self.silver_database}.{self.silver_table}").replace('@temp_table', self.temp_table)
    
    def optimize_write(self, batchId):

        logger = self.logger
        logger.info(f"batchId is {batchId}")
        mod = batchId%100
        logger.info(f"batch mod is {mod}")
        
        if mod == 0:
            logger.info(f"OPTIMIZING TABLE {self.silver_database}.{self.silver_table}")
            try:
                self.spark.sql(f"OPTIMIZE {self.silver_database}.{self.silver_table}")
            except Exception as e:
                logger.error(e.with_traceback)

    def _append_to_delta(self, micro_df: DataFrame, batch_id: int) -> None:    

        micro_df.write.format("delta").mode("append").saveAsTable(f"{self.silver_database}.{self.silver_table}")
        self.optimize_write(batch_id)
        
    def _upsert_to_delta(self, micro_df: DataFrame, batch_id: int) -> None:

        sequence_cols = self._generate_sequence_cols(self.sequence_dict)

        source_df = self._dedup_dataframe(micro_df, part_cols=self.merge_cols, sequence_cols=sequence_cols)

        if self.silver_custom_merge_path is not None:

            """Need to create a global temp table as streaming query does not use the same Spark session as the originating session"""
            source_df.createOrReplaceGlobalTempView(self.temp_table)
            """Override"""
            self.spark.sql(self.merge_statement) 

        else:

            merge_condition = self._generate_merge_condition(self.merge_cols)
            update_condition = self._generate_update_condition(self.sequence_dict)
            
            insert_values = {c: f"s.{c}" for c in micro_df.columns}
            update_values = {c: f"s.{c}" for c in micro_df.columns if c not in self.merge_cols}

            self.target_table.alias("t").merge(
                source = source_df.alias("s"),
                condition = merge_condition
            ) \
            .whenMatchedUpdate(condition = update_condition,
                                set = update_values
                                ) \
            .whenNotMatchedInsert(values = insert_values) \
            .execute()

        self.optimize_write(batch_id)
        
    def write(self) -> StreamingQuery:        
        trigger_interval = get_trigger_interval(self.trigger_interval)
        
        stream_writer = (self._read()
                      .transform(self._silver_preprocessing)
                      .writeStream 
                      .queryName(f"{self.target_table_name}_stream")
                      .format("delta") 
                      .option("checkpointLocation", self.silver_checkpoint) 
                  )

        if trigger_interval.lower() == 'availablenow':
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            stream_writer = stream_writer.trigger(processingTime = trigger_interval)
        
        if (self.merge_cols is None) or (len(self.merge_cols) == 0): 
            stream = stream_writer.foreachBatch(lambda df, batchId: self._append_to_delta(df, batchId)).start()
        else: 
            stream = stream_writer.foreachBatch(self._upsert_to_delta).start()
                     
        if self.is_batch:
            stream = stream.awaitTermination()

        return stream
    

