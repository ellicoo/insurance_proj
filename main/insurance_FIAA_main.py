from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import decimal
import pandas as pd
from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO： 执行sql文件的工具类
   SourceFile  :	execute_sql
   Author      :	Denny
   Date	       :	2023/6/25
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.构建SparkSession对象
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置即可
spark = SparkSession.builder.appName("insurance_FIAA_main") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://node1:9083") \
    .enableHiveSupport() \
    .getOrCreate()

#2.工具类
#功能: 读取外部SQL脚本文件, 识别每一个SQL语句, 去重SQL中空行注释, 然后执行SQL语句  如果SQL以select开头, 打印其返回的结果
def executeSQLFile(filename):
    with open(r'../sql_script/' + filename, 'r') as f:
        # 读取文件中所有行数据, 得到一个列表,列表中每一个元素就是一行行数据
        read_data = f.readlines()
        # 将数组的一行一行拼接成一个长文本，就是SQL文件的内容
        read_data = ''.join(read_data)
        # 将文本内容按分号切割得到数组，每个元素预计是一个完整语句
        arr = read_data.split(";")
        # 对每个SQL,如果是空字符串或空文本，则剔除掉
        # 注意，你可能认为空字符串''也算是空白字符，但其实空字符串‘’不是空白字符 ，即''.isspace()返回的是False
        arr2 = list(filter(lambda x: not x.isspace() and not x == "", arr))
        # 对每个SQL语句进行迭代
        for sql in arr2:
            # 先打印完整的SQL语句。
            print(sql, ";")
            # 由于SQL语句不一定有意义，比如全是--注释;，他也以分号结束，但是没有意义不用执行。
            # 对每个SQL语句，他由多行组成，sql.splitlines()数组中是每行，挑选出不是空白字符的，也不是空字符串''的，也不是--注释的。
            # 即保留有效的语句。
            filtered = filter(
                lambda x: (not x.lstrip().startswith("--")) and (not x.isspace()) and (not x.strip() == ''),
                sql.splitlines())
            # 下面数组的元素是SQL语句有效的行
            filtered = list(filtered)

            # 有效的行数>0，才执行
            if len(filtered) > 0:
                df = spark.sql(sql)
                # 如果有效的SQL语句是select开头的，则打印数据。
                if filtered[0].lstrip().startswith("select"):
                    df.show()


#3.执行SQL文件
# executeSQLFile('01_insurance_ods_table.sql')
executeSQLFile('02_insurance_dw_table.sql')


# 4.关闭SparkSession对象
spark.stop()
