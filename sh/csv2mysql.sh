#!/bin/bash

CSV_PATH=$1
DEL_DT=$2

user="root"
database="history_db"

MYSQL_PWD='qwer123'  mysql --local-infile=1 -u"$user"<<EOF
-- mysql --local-infile=1 -u"$user" -p"$password" "$database" <<EOF
-- LOAD DATA LOCAL INFILE '/home/kim1/data/csv/20240718/csv.csv'

DELETE FROM history_db.tmp_cmd_usage WHERE dt='$DEL_DT';

LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1
FIELDS
	TERMINATED BY ','
	ESCAPED BY '\b'
	ENCLOSED BY '^'
LINES TERMINATED BY '\n';
EOF
