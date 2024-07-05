CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` ( `sensorReadingTime` bigint, `serialNumber` string, `distanceFromObject` float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://data-lake-estudo-udacity/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'parquet')