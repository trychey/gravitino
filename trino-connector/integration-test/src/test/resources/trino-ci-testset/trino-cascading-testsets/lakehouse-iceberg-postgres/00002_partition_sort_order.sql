USE gt_iceberg_postgresql1.gt_db2;

select * from lineitem order by orderkey, partkey limit 5;

select * from tb03;

SHOW CREATE SCHEMA gt_iceberg_postgresql1.gt_db2;

SHOW SCHEMAS LIKE 'gt_%2';

SHOW TABLES LIKE '%item';

SHOW COLUMNS FROM gt_iceberg_postgresql1.gt_db2.tb01;