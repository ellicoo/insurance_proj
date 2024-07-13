-- dw层构建
create database if not exists insurance_dw;
--1.保险中对数据精度要求非常高。
set spark.sql.shuffle.partitions=4;
-- 设置禁止精度损失
set spark.sql.decimalOperations.allowPrecisionLoss=false;

--创建保费参数因子表（用于计算保费表）
create table if not exists insurance_dw.prem_src(
    age_buy smallint comment '投保年龄',
    Nursing_Age smallint comment '长期护理保险金给付期满年龄',
    sex string comment '性别',
    T_Age smallint comment '满期年龄',
    PPP smallint comment '缴费期间',
    BPP smallint comment '保险期间',
    interest_rate decimal(6,4) comment '预定利息率',
    sa smallint comment '基本保险金额',
    policy_year smallint comment '保单年度',
    age smallint comment '年龄',
    qx decimal(17,12) comment '死亡率',
    kx decimal(17,12) comment '残疾死亡占死亡的比例',
    qx_d decimal(17,12) comment '扣除残疾的死亡率',
    qx_ci decimal(17,12) comment '残疾率qx_ci',
    dx_d	decimal(17,12) ,
    dx_ci	decimal(17,12),
    lx decimal(17,12) comment '有效保单数',
    lx_d decimal(17,12) comment '健康人数',
    Cx	decimal(17,12) ,
    Cx_	decimal(17,12) ,
    Ci_Cx	decimal(17,12) ,
    Ci_Cx_	decimal(17,12) ,
    Dx decimal(17,12) comment '有效保单生存因子',
    Dx_D_ decimal(17,12) comment '健康人数生存因子',
    PPP_ decimal(17,12) comment '缴费期间',
    BPP_ decimal(17,12) comment '保险期间',
    Expense decimal(17,12) comment '附加费用率',
    DB1 decimal(17,12) comment '残疾给付',
    db2_factor decimal(17,12) comment '长期护理保险金给付因子',
    DB2 decimal(17,12) comment '长期护理保险金',
    DB3 decimal(17,12) comment '养老关爱金',
    DB4 decimal(17,12) comment '身故给付保险金',
    DB5 decimal(17,12) comment '豁免保费因子'
) comment '保费参数因子表'
row format delimited fields terminated by '\t';

--构建dw层的表
--dw层的表是由10个维度和23个指标构成
--构建维度10个维度
--10个维度中，有4个可变维度，6个固定维度
--投保年龄(18-60岁)
create or replace view insurance_dw.prem_src0_age_buy as
    select explode(sequence(18,60)) as age_buy;

--缴费期(10,15,20,30)
create or replace view insurance_dw.prem_src0_ppp as
    select stack(4,10,15,20,30) as ppp;

--性别
create or replace view insurance_dw.prem_src0_sex as
select stack(2,"M","F") as sex;

--保单年度
create or replace view insurance_dw.prem_src0_policy_year as
select explode(sequence(1,88)) as policy_year;

--组装维度(274，19338条)
--性别、缴费期、投保年龄这三个维度可以有274条数据。
--性别、缴费期、投保年龄、保单年度这四个维度可以有19338条数据。

-- 70岁不能缴费
-- 缴费期10年，允许年龄18～70-10，即60岁的人购买，投保年龄prem_src0_age_buy -- age_buy
-- 缴费期15年，允许年龄18～70-15，即55岁的人购买，投保年龄prem_src0_age_buy -- age_buy
-- 缴费期20年，允许年龄18～70-20，即50岁的人购买，投保年龄prem_src0_age_buy -- age_buy
-- 缴费期30年，允许年龄18～70-30，即40岁的人购买，投保年龄prem_src0_age_buy -- age_buy

select
    t1.sex,
    t2.ppp,
    t3.age_buy,
    t4.policy_year
from insurance_dw.prem_src0_sex t1 join insurance_dw.prem_src0_ppp t2 on 1 = 1
join insurance_dw.prem_src0_age_buy t3 on t3.age_buy >= 18 and t3.age_buy <= 70 - t2.ppp
join insurance_dw.prem_src0_policy_year t4 on t4.policy_year >= 1 and t4.policy_year <= 106 - t3.age_buy;

--常量表
create or replace view insurance_dw.input as
select
    0.035 interest_rate,
    0.055 Interst_Rate_CV,
    0.0004 Acci_qx,
    0.115 RDR,
    10000 sa,
    0 Average_Size,
    1 MortRatio_Prem_0,
    1 MortRatio_RSV_0,
    1 MortRatio_CV_0,
    1 CI_RATIO,
    6 B_time1_B,
    59 B_time1_T,
    0.1 B_ratio_1,
    60 B_time2_B,
    106 B_time2_T,
    0.1 B_ratio_2,
    70 MB_TIME,
    0.2 MB_Ration,
    0.7 RB_Per,
    0.7 TB_Per,
    1 Disability_Ratio,
    0.1 Nursing_Ratio,
    75 Nursing_Age;

select * from insurance_dw.input;

--完善维度组装（4个可变维度+6个固定不变的维度）
create or replace view insurance_dw.prem_src0 as
select
    t3.age_buy,
    input.Nursing_Age,
    t1.sex,
    input.B_time2_T as T_Age,
    t2.ppp as ppp,
    106 - t3.age_buy as bpp,
    input.interest_rate,
    input.sa,
    t4.policy_year,
    (t3.age_buy + t4.policy_year - 1) as age
from insurance_dw.prem_src0_sex t1 join insurance_dw.prem_src0_ppp t2 on 1 = 1
join insurance_dw.prem_src0_age_buy t3 on t3.age_buy >= 18 and t3.age_buy <= 70 - t2.ppp
join insurance_dw.prem_src0_policy_year t4 on t4.policy_year >= 1 and t4.policy_year <= 106 - t3.age_buy
join insurance_dw.input input on 1 = 1;

select count(1) from insurance_dw.prem_src0;

-- 是否在缴费期间内ppp_
-- 是否在保费期间内bpp_
create or replace view insurance_dw.prem_src1 as
select
    *,
    if(t1.policy_year <= t1.ppp, 1, 0) as ppp_,
    if(t1.policy_year <= t1.bpp, 1, 0) as bpp_
from insurance_dw.prem_src0 as t1;
--校验
select * from insurance_dw.prem_src1 where age_buy = 18 and sex = 'M' and ppp = '20';

--一定要多校验，可以从上面、中间、最后三部分来校验。


-- 死亡率qx、残疾死亡占死亡的比例kx、残疾率qx_ci
create or replace view insurance_dw.prem_src2 as
select
    t1.*,
    if(t1.age <= 105, if(t1.sex = 'M', t3.cl1, t3.cl2), 0) * t2.MortRatio_Prem_0 * t1.bpp_ as qx,
    if(t1.age <= 105, if(t1.sex = 'M', t4.k_male, t4.k_female), 0) * t1.bpp_ as kx,
    if(t1.sex = 'M', t4.male, t4.female) * t1.bpp_ as qx_ci
from insurance_dw.prem_src1 as t1 join insurance_dw.input t2 on 1 = 1
join insurance_ods.mort_10_13 t3 on t1.age = t3.age
join insurance_ods.dd_table t4 on t1.age = t4.age;
--校验
select * from insurance_dw.prem_src2 where age_buy = 18 and sex = 'M' and ppp = '20';

-- 扣除残疾的死亡率
create or replace view insurance_dw.prem_src3 as
select
    *,
    if(age = 105, qx - qx_ci, qx * (1 - kx)) * bpp_ as qx_d
from insurance_dw.prem_src2;
--校验
select * from insurance_dw.prem_src3 ;

-- 第四步
create or replace view prem_src4_1 as
       select
       *,
       if(policy_year = 1, 1, null) as lx
    from prem_src3;

create  or replace view  insurance_dw.prem_src4
        select
            age_buy,
            Nursing_Age,
            sex,
            T_Age,
            ppp,
            bpp,
            interest_rate,
            sa,
            policy_year,
            age,
            ppp_,
            bpp_,
            qx,
            kx,
            qx_ci,
            qx_d,
           cast (spark_udaf(qx,lx) over(partition by age_buy,sex,ppp order by policy_year) as decimal(17,12)) as lx
        from prem_src4_1;



