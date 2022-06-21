CREATE EXTERNAL TABLE IF NOT EXISTS `db_refined.test_table`(
  `date_key` date, 
  `user_id` string, 
  `country` string
)
STORED AS PARQUET
LOCATION
  's3://#BUCKET/test_table'