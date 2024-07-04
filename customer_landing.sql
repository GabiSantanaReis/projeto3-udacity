CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`costumer_landing` (
  `serialnumber` string,
  `sharewithpublicasofdate` bigint,
  `birthday` string,
  `registrationdate` string,
  `sharewithresearchasofdate` bigint,
  `customername` string,
  `email` string,
  `lastupdatedate` bigint,
  `phone` string,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'

WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://data-lake-estudo-udacity/customer/landing/'
TBLPROPERTIES ('classification' = 'json');