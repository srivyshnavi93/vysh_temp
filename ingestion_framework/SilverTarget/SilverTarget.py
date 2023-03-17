from ingestion_framework.Includes.Utils import get_trigger_interval
from pyspark.sql.window import Window as W
from pyspark.sql.functions import col, from_json, explode, posexplode, expr
from pyspark.sql.types import StructType, StringType
from pyspark.sql import Column, Row, DataFrame, SparkSession
from dataclasses import dataclass
from typing import Iterable, Optional
from abc import ABC, abstractmethod
import json
import pyspark.sql.functions as F

@dataclass 
class SilverTarget(ABC): 
    bronze_database:str
    bronze_table:str
    silver_checkpoint:str
    silver_trans_path:str
    use_schema:bool
    schema_json_loc: str
    trigger_interval:Optional[str]
    max_bytes_per_trigger:Optional[str]
    delta_starting_timestamp_list:list
    spark:SparkSession
    
    @classmethod 
    @abstractmethod
    def from_row(cls, row: Row, spark: SparkSession): 
        raise NotImplementedError(f"No row constructor for {cls.__name__}")
        
    def __post_init__(self):
        """
        Initialization method that reads from the specificed silver transformation 
        json file and loads its values as class attributes
        
        The silver transformation file is copied from its remote location to the 
        local driver, where it is read and parsed 
        """

        log4j = self.spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(__name__)

        self.delta_starting_timestamp = None

        if self.use_schema:
            self.schema = self.read_schema(self.schema_json_loc)      
            
        with open(self.silver_trans_path) as fp:
            silver_transform_json = json.load(fp)

        explode_cols = silver_transform_json.get("explode", None)
        pos_explode_cols = silver_transform_json.get("pos_explode", None)    
            
        select_expr_cols = silver_transform_json.get("select", None)
        where_clauses = silver_transform_json.get("where", None)
        
        ## array of columns
        merge_cols = silver_transform_json.get("merge_cols", None)
        
        ## array of 1 element with additional MERGE join expr - mostly intended for insert-only tables
        sink_filter = silver_transform_json.get("sink_filter", None)
        
        ## dict of {column_name: is_ascending}
        sequence_dict = silver_transform_json.get("sequence_dict", None)

        stream_filter = silver_transform_json.get("stream_filter", None)
        stream_dedup = silver_transform_json.get("stream_dedup", None)    

        watermark_col = silver_transform_json.get("watermark_col", "timestamp")            
        
        self.source_name = f"`{self.bronze_database}`.`{self.bronze_table}`"

        self.explode_cols = explode_cols 
        self.pos_explode_cols = pos_explode_cols
        
        self.select_expr_cols = select_expr_cols 
        self.where_clauses = where_clauses
        
        self.merge_cols = merge_cols 
        self.sink_filter = sink_filter
        
        self.sequence_dict = sequence_dict

        self.stream_filter = stream_filter
        self.stream_dedup = stream_dedup
        
        self.watermark_col = watermark_col

        trigger_interval = get_trigger_interval(self.trigger_interval)

        if trigger_interval.lower() == 'availablenow':
            self.is_batch = True
        else:
            self.is_batch = False
        
        if self.delta_starting_timestamp_list[0] is None and len(self.delta_starting_timestamp_list[1]) > 0:
            self.logger.warn(f"delta_starting_timestamp value provided for {self.bronze_database}.{self.bronze_table} was not in correct format.  Required format is YYYY-MM-DD HH:MM:SS")
        elif self.delta_starting_timestamp_list[0] is not None:
            self.logger.warn(f"starting delta streaming pipeline for table: {self.bronze_database}.{self.bronze_table} from timestamp {self.delta_starting_timestamp_list[0]}")
            self.delta_starting_timestamp = self.delta_starting_timestamp_list[0]

    def read_schema(self, schema_json_loc: str) -> StructType: 

        assert schema_json_loc.endswith(".json")

        with open(schema_json_loc) as fp: 
            json_text = json.load(fp)

        return StructType.fromJson(json_text)
        
    def _generate_merge_condition(self, merge_cols: Iterable[str], sink_filter: Iterable[str]) -> str: 
        merge_condition = " AND ".join(f"s.{c} = t.{c}" for c in merge_cols)
        if sink_filter:
            merge_condition += " AND " + " AND ".join(f"t.{c}" for c in sink_filter)
        return merge_condition
    
    def _generate_update_condition(self, update_col_dict: dict) -> str: 
        equality_array = ["<" if acc else ">=" for acc in update_col_dict.values()]
        return " AND ".join( [f"s.{c} {eq} t.{c}" for eq, c  in zip(equality_array, update_col_dict.keys())])
    
    def _generate_sequence_cols(self, update_col_dict: dict) -> Iterable[Column]: 
        return [expr(c) if acc else expr(c).desc() for c, acc in update_col_dict.items()]
    
    def _dedup_dataframe(self, input_df: DataFrame, part_cols: Iterable[Column], sequence_cols: Iterable[Column]) -> DataFrame: 

        rn_col = "rn"
        assert rn_col not in input_df.columns, f"Problem with temp column {rn_col} already existing"
        rn_window = W.partitionBy(*part_cols).orderBy(*sequence_cols)

        return input_df.withColumn(rn_col, F.row_number().over(rn_window)).where(col(rn_col) == 1).drop(rn_col)
    
    def _silver_preprocessing(self, input_df: DataFrame) -> DataFrame: 
            
        if self.explode_cols:
            for array_field, field_name in self.explode_cols.items():
                input_df = input_df.withColumn(field_name, explode(expr(array_field)))
                
        if self.pos_explode_cols:
            for array_field, field_names in self.pos_explode_cols.items():
                pos, col_name = field_names.split(';')
                input_df = input_df.select("*", posexplode(expr(array_field)).alias(pos, col_name))
        
        if self.select_expr_cols is not None: 
            input_df = input_df.selectExpr(*self.select_expr_cols)
        
        if self.where_clauses is not None: 
            for w in self.where_clauses: 
                input_df = input_df.where(w)

        return input_df 
    
    def _read(self) -> DataFrame: 

        read_df = self.spark.readStream.format("delta").option("ignoreDeletes", "true")

        if self.max_bytes_per_trigger is not None:
            read_df = read_df.option("maxBytesPerTrigger", self.max_bytes_per_trigger)

        if self.delta_starting_timestamp is not None:
            read_df = read_df.option("startingTimestamp", self.delta_starting_timestamp)

        read_df = read_df.table(self.source_name)

        if self.stream_filter is not None: 
            for f in self.stream_filter: 
                read_df = read_df.filter(f)
        
        if self.stream_dedup is not None: 
            read_df = read_df.withWatermark(self.watermark_col, "3 days").drop_duplicates(subset=self.stream_dedup)
        
        if self.use_schema:
            return read_df.withColumn("payload", from_json(col("value").cast(StringType()), self.schema))
        else:
            return read_df.withColumn("payload", col("value").cast(StringType()))
    
    @abstractmethod 
    def write(cls): 
        raise NotImplementedError(f"No write method for {cls.__name__}")