--1.创建Hive的数据库
create database if not exists insurance_ods;
--2.创建ODS层的表，用来接收数据
-- 构建表area
create table if not exists insurance_ods.area(
id int,
province string,
city string,
direction string
) comment '全国行政地区表'
row format delimited fields terminated by '\t';



--构建表claim_info
create table if not exists insurance_ods.claim_info(
    pol_no string,
    user_id string,
    buy_datetime string,
    insur_code string,
    claim_date string,
    claim_item string,
    claim_mnt decimal(16,4)
)comment '理赔信息表'
row format delimited fields terminated by '\t';


--构建mort_10_13
create table if not exists insurance_ods.mort_10_13(
age int,
cl1 decimal(10,8),
cl2 decimal(10,8),
cl3 decimal(10,8),
cl4 decimal(10,8),
cl5 decimal(10,8),
cl6 decimal(10,8)
)comment '中国人身保险业经验生命表（2010－2013）'
row format delimited fields terminated by '\t';


--构建表dd_table
create table if not exists insurance_ods.dd_table(
age int,
male decimal(10,8),
female decimal(10,8),
k_male decimal(10,8),
k_female decimal(10,8)
)comment '行业25种重疾发生率'
row format delimited fields terminated by '\t';


--构建表pre_add_exp_ratio
create table if not exists insurance_ods.pre_add_exp_ratio(
PPP int,
r1 decimal(10,8),
r2 decimal(10,8),
r3 decimal(10,8),
r4 decimal(10,8),
r5 decimal(10,8),
r6_ decimal(10,8),
r_avg decimal(10,8),
r_max decimal(10,8)
)comment '预定附加费用率'
row format delimited fields terminated by '\t';


--构建表prem_std_real
create table if not exists insurance_ods.prem_std_real(
age_buy int,
sex string,
ppp int,
bpp string,
prem decimal(14,6)
)comment '标准保费真实参照表'
row format delimited fields terminated by '\t';


--构建表prem_cv_real
create table if not exists insurance_ods.prem_cv_real(
age_buy int,
sex string,
ppp int,
prem_cv decimal(15,7)
)comment '保单价值准备金毛保费，真实参照表'
row format delimited fields terminated by '\t';


--构建表policy_client
create table if not exists insurance_ods.policy_client(
user_id string,
name string,
id_card string,
phone string,
sex string,
birthday string,
province string,
city string,
direction string,
income int
)comment '客户信息表'
row format delimited fields terminated by '\t';


--构建表policy_benefit
create table if not exists insurance_ods.policy_benefit(
pol_no string,
user_id string,
ppp int,
age_buy int,
buy_datetime string,
insur_name string,
insur_code string,
pol_flag smallint,
elapse_date string
)comment '客户投保详情表'
row format delimited fields terminated by '\t';


--构建表policy_surrender
create table if not exists insurance_ods.policy_surrender(
pol_no string,
user_id string,
buy_datetime string,
keep_days smallint,
elapse_date string
)comment '退保纪录表'
row format delimited fields terminated by '\t';


