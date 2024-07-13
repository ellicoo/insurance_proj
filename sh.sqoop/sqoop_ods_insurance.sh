#！/bin/bash
#同步area表
sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table area \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table area \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table claim_info \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table claim_info \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table mort_10_13 \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table mort_10_13 \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table dd_table \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table dd_table \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table pre_add_exp_ratio \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table pre_add_exp_ratio \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table prem_std_real \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table prem_std_real \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table prem_cv_real \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table prem_cv_real \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table policy_client \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table policy_client \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table policy_benefit \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table policy_benefit \
--fields-terminated-by '\t' \
-m 1

wait

sqoop import \
--connect jdbc:mysql://node1:3306/insurance \
--username root \
--password 123456 \
--table policy_surrender \
--hive-import \
--hive-overwrite \
--hive-database insurance_ods \
--hive-table policy_surrender \
--fields-terminated-by '\t' \
-m 1
