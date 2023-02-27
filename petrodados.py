from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.functions import expr
Command took 0.12 seconds -- by alvarotecno4@gmail.com at 27/02/2023, 13:33:56 on My Cluster
petrodados = spark.read.format("csv").option("header", "true").option("sep",";").load("dbfs:/FileStore/shared_uploads/alvarotecno4@gmail.com/dadospetrobras-1.csv")
(1) Spark Jobs
petrodados:pyspark.sql.dataframe.DataFrame = [BLOCO: string, REGIÃO GEOGRÁFICA: string ... 3 more fields]

Importando Dados

petrodados.show(5)
(1) Spark Jobs
+--------+-----------------+------+----+--------------------+
|   BLOCO|REGIÃO GEOGRÁFICA|  PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+--------+-----------------+------+----+--------------------+
|NÃO OPEP| América do Norte|Canadá|2012|                3740|
|NÃO OPEP| América do Norte|Canadá|2013|         4000,410965|
|NÃO OPEP| América do Norte|Canadá|2014|         4270,529903|
|NÃO OPEP| América do Norte|Canadá|2015|         4388,135578|
|NÃO OPEP| América do Norte|Canadá|2016|         4463,638847|
+--------+-----------------+------+----+--------------------+
only showing top 5 rows


RETIRADA DA COLUNA BLOCO

petrodados.orderBy("PRODUÇÃO DE PETRÓLEO").show(5)
(1) Spark Jobs
+--------+--------------------+-----------+----+--------------------+
|   BLOCO|   REGIÃO GEOGRÁFICA|       PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+--------+--------------------+-----------+----+--------------------+
|NÃO OPEP|              Europa|     Itália|2021|         100,3936251|
|NÃO OPEP|       Oriente Médio|        Omã|2016|         1004,262568|
|NÃO OPEP|              Europa|Reino Unido|2017|         1005,425044|
|NÃO OPEP|Américas Central ...|   Colômbia|2015|         1005,574436|
|NÃO OPEP|Américas Central ...|   Colômbia|2013|         1009,871696|
+--------+--------------------+-----------+----+--------------------+
only showing top 5 rows


dados = petrodados.select("PAÍS","ANO", "PRODUÇÃO DE PETRÓLEO")
dados:pyspark.sql.dataframe.DataFrame = [PAÍS: string, ANO: string ... 1 more field]

EXCLUINDO COLUNA "BLOCO"

petrodados.drop("BLOCO").show(5)
(1) Spark Jobs
+-----------------+------+----+--------------------+
|REGIÃO GEOGRÁFICA|  PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+-----------------+------+----+--------------------+
| América do Norte|Canadá|2012|                3740|
| América do Norte|Canadá|2013|         4000,410965|
| América do Norte|Canadá|2014|         4270,529903|
| América do Norte|Canadá|2015|         4388,135578|
| América do Norte|Canadá|2016|         4463,638847|
+-----------------+------+----+--------------------+
only showing top 5 rows


dados.orderBy("PRODUÇÃO DE PETRÓLEO").show(5)
(1) Spark Jobs
+-----------+----+--------------------+
|       PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+-----------+----+--------------------+
|     Itália|2021|         100,3936251|
|        Omã|2016|         1004,262568|
|Reino Unido|2017|         1005,425044|
|   Colômbia|2015|         1005,574436|
|   Colômbia|2013|         1009,871696|
+-----------+----+--------------------+
only showing top 5 rows


agrupados = dados.orderBy("ANO","PAÍS")
agrupados.show(5)
(1) Spark Jobs
agrupados:pyspark.sql.dataframe.DataFrame = [PAÍS: string, ANO: string ... 1 more field]
+--------------+----+--------------------+
|          PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+--------------+----+--------------------+
|        Angola|2012|                1734|
|     Argentina|2012|                 657|
|       Argélia|2012|                1537|
|Arábia Saudita|2012|               11622|
|     Austrália|2012|                 472|
+--------------+----+--------------------+
only showing top 5 rows


petrodados.show(5)
(1) Spark Jobs
+--------+-----------------+------+----+--------------------+
|   BLOCO|REGIÃO GEOGRÁFICA|  PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+--------+-----------------+------+----+--------------------+
|NÃO OPEP| América do Norte|Canadá|2012|                3740|
|NÃO OPEP| América do Norte|Canadá|2013|         4000,410965|
|NÃO OPEP| América do Norte|Canadá|2014|         4270,529903|
|NÃO OPEP| América do Norte|Canadá|2015|         4388,135578|
|NÃO OPEP| América do Norte|Canadá|2016|         4463,638847|
+--------+-----------------+------+----+--------------------+
only showing top 5 rows


petrodados.drop("BLOCO").show(5)
(1) Spark Jobs
+-----------------+------+----+--------------------+
|REGIÃO GEOGRÁFICA|  PAÍS| ANO|PRODUÇÃO DE PETRÓLEO|
+-----------------+------+----+--------------------+
| América do Norte|Canadá|2012|                3740|
| América do Norte|Canadá|2013|         4000,410965|
| América do Norte|Canadá|2014|         4270,529903|
| América do Norte|Canadá|2015|         4388,135578|
| América do Norte|Canadá|2016|         4463,638847|
+-----------------+------+----+--------------------+
only showing top 5 rows


media = petrodados.groupBy(petrodados["ANO"]).agg(sum("PRODUÇÃO DE PETRÓLEO")).show()
(2) Spark Jobs
+----+-------------------------+
| ANO|sum(PRODUÇÃO DE PETRÓLEO)|
+----+-------------------------+
|2016|                     25.0|
|2012|                  86210.0|
|2020|                   3084.0|
|2019|                   3399.0|
|2017|                   4854.0|
|2014|                     33.0|
|2013|                     null|
|2018|                   4608.0|
|2021|                   4075.0|
|2015|                     27.0|
+----+-------------------------+
