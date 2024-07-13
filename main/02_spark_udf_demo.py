# coding:utf8
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：SparkUDF函数回顾，把user.txt中的name字段拼接一个"_itcast"字符串
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
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("SparkSQLAppName") \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

    # 2.数据输入
    # header：是否使用文件中的第一行作为DF的Schema
    # inferSchema：自动推断数据类型
    input_df = spark.read.csv(path='../data/user.txt', sep=',', header=True, inferSchema=True)


    # 3.数据处理
    # 3.1定义UDF函数
    def spark_udf(name: str):
        return name + "_itcast"


    # 3.2 注册UDF函数
    # 注册函数有三种方式
    # 方式一：register（SQL、DSL）
    spark.udf.register("spark_udf_sql", spark_udf, returnType=StringType())
    # 方式二：F.udf（DSL）
    # udf_dsl = F.udf(spark_udf,returnType=StringType())
    # 方式三：语法糖（DSL）--装饰器模式
    # @F.udf(returnType=StringType())

    # SQL的使用方式：二步走
    # 步骤一，把DF注册为视图
    input_df.createOrReplaceTempView("t1")
    # 步骤二：写SQL
    result_df = spark.sql("select user_id,name,spark_udf_sql(name) as concat_name from t1")

    # 4.数据输出
    input_df.printSchema()
    input_df.show()
    result_df.printSchema()
    result_df.show()
    # 5.关闭SparkContext
    spark.stop()
