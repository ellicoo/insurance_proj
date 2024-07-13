# coding:utf8
import os

from pandas import Series
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
import pandas as pd

import pyspark.sql.functions as F

"""
#说你人话：pyspark.sql.functions包里面的函数可以在SparkSQL中使用,其操作的返回值大多数是Column对象
"""

"""
-------------------------------------------------
   Description :	TODO：SparkSQL模版
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 共享变量--driver中的本地数据和executor中的rdd数据需要一起进行运算时使用
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

"""
DataFrame的组成：
1、结构层面：
（1）StrucType对象描述整个DataFrame的表结构
（2）StrucField对象描述一个列的信息
2、数据层面：
（1）Row对象记录一行数据
（2）Column对象记录一列数据并包含列的信息

"""

if __name__ == '__main__':
    # 1.构建SparkSession
    # 建造者模式：类名.builder.配置…….getOrCreate()
    # 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
    spark = SparkSession.builder.appName("04_udaf_of_ valid_prem_src") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://node1:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 2.数据输入
    spark.sql("show databases").show()
    spark.sql("use insurance_dw").show()
    spark.sql("show tables").show()
    spark.sql("select * from prem_src3").show()

    # 3.数据处理
    # 创建并初始化有效保单数lx列
    spark.sql("""
    create or replace view prem_src4_1 as
       select 
       *,
       if(policy_year = 1, 1, null) as lx
    from prem_src3;
    """)

    # 自定义处理函数
    @F.pandas_udf(returnType=FloatType())
    def spark_udaf_func(qx: Series, lx: Series) -> float:
        # 计算逻辑：当第一保单年度时，即policy_year=1时，lx=1
        # 在第2及后续保单年度时，即policy_year>=2时
        # lx=上一年度的lx*(1-上一年度的qx)
        tmp_lx = 0
        for i in range(len(lx)):
            if i == 0:
                tmp_lx = float(lx[i])
            else:
                tmp_lx = tmp_lx * (1 - float(qx[i - 1]))
        return tmp_lx


    spark.udf.register("spark_udaf", spark_udaf_func)


    spark.sql("""
    create table if not exists insurance_dw.prem_src4
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
    """).show()

    # 4.数据输出


    # 5.关闭SparkContext
    spark.stop()
