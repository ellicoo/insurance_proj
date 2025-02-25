# coding:utf8
import os

from pandas import Series
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
import pyspark.sql.functions as F
import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：udaf
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
"""
UDAF--自定义聚合函数
SparkSQL同时支持UDF和UDAF。但是对于Python而言，仅仅只支持UDF，不支持UDAF函数。

如果想使用Python语言编写UDAF函数，则比需借助于Python的第三方包Pandas的UDF函数来实现。

自定义函数的步骤如下：
#1.第一步
根据功能要求，定义一个普通的Python的函数

#2.第二步
将这个python的函数注册到SparkSQL中。
注册方式有以下三种方案：
#2.1方式一，可适用于SQL和DSL
	udf对象 = sparkSession.udf.register(参数1, 参数2,参数3)
	参数1: udf函数的函数名称, 此名称用于在SQL风格中使用
	参数2: 需要将哪个python函数注册为udf函数
	参数3: 设置python函数返回的类型
	udf对象 主要使用在DSL中

#2.2方式二，仅适用于DSL方案
	udf对象 =F.udf(参数1,参数2)
	参数1: 需要将哪个python函数注册为udf函数
	参数2: 设置python函数返回的类型
	udf对象 主要使用在DSL中
	
#2.3方式三，语法糖写法:@F.udf(设置返回值类型)，仅适用于DSL方案
	放置在普通的python函数的上面

#3.在SQL或者DSL中使用即可

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

    spark.sql("""
    -- 初始化数据:
    create or replace temporary view t1(c1,c2,c3) as
        values (1,1,6),
               (1,2,23),
               (1,3,8),
               (1,4,4),
               (1,5,10),
               (2,1,23),
               (2,2,14),
               (2,3,17),
               (2,4,20);
    """)

    spark.sql("""
    create or replace temporary view t2 as
        select
               c1,
               c2,
               c3,
               if(c2 = 1, 1, null) as c4
        from t1;
    """)


    # 需求：通过pandas的UDF函数来实现UDAF函数
    # 步骤：和普通的UDF函数一样
    # 步骤一：自定义一个Python的UDF函数
    # 如果想使用Pandas的UDF函数，则要保证数据列的类型必须是Series类型
    # spark中的DataFrame类的列叫Colnmu。本质也是数组，都可以

    # 要使用上一个C4
    # C3\C4本身存在才可以

    # Series 是 Pandas 的一维数组，不是python的数组
    # Spark 中的 pandas_udf 是一种分布式操作
    # spark.sql.function中的pandas是分布式操作的pandas，跟非spark的独立Pandas类型相同，仅仅分布式不同，独立pandas是单机处理的
    # 「注意」在 Spark 中使用 pandas_udf 来处理 DataFrame 的原因主要是为了结合 Pandas 的灵活性和 Spark 的分布式计算能力，
    # 虽然 Spark 自身的 API 已经非常强大，但在某些情况下，Pandas 的操作可能更简洁和高效。
    # 许多数据科学和数据分析库（如 Scikit-learn）都是基于 Pandas 的，可以直接在 pandas_udf 中使用这些库
    """
    这个UDAF是对每个分组数据的聚合算出c4，不是每行单独作为参数计算得出单独结果，而是整列作为数组输入到函数中，得出结果
    与UDF不同，它是将整个分组的某个列作为参数输入，即Series对象是
    @F.udf
    基本概念：@F.udf 装饰器用于定义一个普通的用户定义函数（UDF），该函数将在每条记录上逐条执行。
    执行方式：UDF 使用 Python 的原生函数逐条处理数据。由于逐条处理，每条记录的处理可能会有较高的开销，尤其是在数据量较大的情况下。
    应用场景：适用于简单的、需要逐条处理的数据转换任务
    
    @F.pandas_udf
    基本概念：@F.pandas_udf 装饰器用于定义一个 Pandas 用户定义函数（UDF），它使用 Apache Arrow 将数据从 Spark 的 JVM 进程高效地转换到 Python 的
            Pandas 数据结构，并在 Pandas 数据框上进行批量操作。
    执行方式：Pandas UDF 在每个分区上批量处理数据，而不是逐条处理。这种批量处理方式可以显著提高性能，尤其是在需要对数据进行复杂操作时。
    应用场景：适用于复杂的数据处理任务，需要利用 Pandas 的强大功能和灵活性的场景
    """
    @F.pandas_udf(returnType=FloatType())
    def spark_udaf_func(c3: Series, c4: Series) -> float:
        # 计算逻辑：当c3=1，则 c4=第一个元素；否则c4 = (上一个c4 + 当前的c3)/2
        # 要求C4，搞临时变量。上一次C4，首次时C4为
        tmp_c4 = 0
        # 不知道C3列list或者说列表的长度不知道，不知道要迭代多少次，使用分组操作后，这个循环只作用在组上
        for i in range(len(c3)):
            if i == 0:
                tmp_c4 = c4[i]
            else:
                tmp_c4 = (tmp_c4 + c3[i]) / 2
        return tmp_c4


    # 步骤二：把Python的UDF函数注册到Spark环境中
    spark.udf.register("spark_udaf", spark_udaf_func)

    spark.sql("""
    select 
        c1,
        c2,
        c3,
        spark_udaf(c3,c4) over(partition by c1 order by c2) as c4
    from t2;
    """).show()

    # 5.关闭SparkContext
    spark.stop()

"""
按照 c1 分组，并在分组内按 c2 排序后，数据分组如下：
分组 1：[1, 1, 3, 4]
       [1, 2, 5, 6]
分组 2：[2, 1, 7, 8]
       [2, 2, 9, 10]
       [2, 3, 11, 12]
c1、c2两列数据变两个等长数组，要操作一行的数据两个数组就需要移动同一个指针和同一步长：
传递给 UDF：
分组 1 的 c3 和 c4 列的数据分别是 [3, 5] 和 [4, 6]。
分组 2 的 c3 和 c4 列的数据分别是 [7, 9, 11] 和 [8, 10, 12]。
执行 UDF 逻辑：

对于分组 1：
第一次迭代：tmp_c4 = c4[0] = 4
第二次迭代：tmp_c4 = (4 + 5) / 2 = 4.5
返回 tmp_c4 = 4.5

对于分组 2：
第一次迭代：tmp_c4 = c4[0] = 8
第二次迭代：tmp_c4 = (8 + 9) / 2 = 8.5
第三次迭代：tmp_c4 = (8.5 + 11) / 2 = 9.75
返回 tmp_c4 = 9.75
"""

