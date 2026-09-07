CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    customername STRING,
    email STRING,
    phone STRING,
    birthday STRING,
    serialnumber STRING,
    registrationdate BIGINT,
    lastupdatedate BIGINT,
    sharewithresearchasofdate BIGINT,
    sharewithpublicasofdate BIGINT,
    sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-shamim/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
