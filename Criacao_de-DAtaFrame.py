from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.functions import expr
Command took 0.07 seconds -- by alvarotecno4@gmail.com at 27/02/2023, 14:19:13 on My Cluster
#CRIANDO UMA TABELA 
schema1 = "ID INT, Nome STRING , Idade INT , Profissao STRING , Sexo STRING , Cidade STRING , Vendas INT"
vendas = [[1,"Alvaro",31, "Analista", "M", "RJ", 234],
          [2,"Janine", 31, "Dona de Casa", "F", "RJ", 245],
          [3,"Renata",25, "Agente de saude", "F", "MG", 242], 
          [4,"Heitor", 11, "Estudante", "M", "MG", 301],   
          [5,"Mellyssa", 23, "Estudante", "F", "RJ", 463], 
          [6,"Jose", 34, "Advogado", "M", "SP", 456]]
df = spark.createDataFrame(vendas, schema1)
df.show() 
df.select("Nome","Vendas").show()
df.orderBy("Vendas").show()
 
(7) Spark Jobs
df:pyspark.sql.dataframe.DataFrame = [ID: integer, Nome: string ... 5 more fields]
+---+--------+-----+---------------+----+------+------+
| ID|    Nome|Idade|      Profissao|Sexo|Cidade|Vendas|
+---+--------+-----+---------------+----+------+------+
|  1|  Alvaro|   31|       Analista|   M|    RJ|   234|
|  2|  Janine|   31|   Dona de Casa|   F|    RJ|   245|
|  3|  Renata|   25|Agente de saude|   F|    MG|   242|
|  4|  Heitor|   11|      Estudante|   M|    MG|   301|
|  5|Mellyssa|   23|      Estudante|   F|    RJ|   463|
|  6|    Jose|   34|       Advogado|   M|    SP|   456|
+---+--------+-----+---------------+----+------+------+

+--------+------+
|    Nome|Vendas|
+--------+------+
|  Alvaro|   234|
|  Janine|   245|
|  Renata|   242|
|  Heitor|   301|
|Mellyssa|   463|
|    Jose|   456|
+--------+------+

+---+--------+-----+---------------+----+------+------+
| ID|    Nome|Idade|      Profissao|Sexo|Cidade|Vendas|
+---+--------+-----+---------------+----+------+------+
|  1|  Alvaro|   31|       Analista|   M|    RJ|   234|
|  3|  Renata|   25|Agente de saude|   F|    MG|   242|
|  2|  Janine|   31|   Dona de Casa|   F|    RJ|   245|
|  4|  Heitor|   11|      Estudante|   M|    MG|   301|
|  6|    Jose|   34|       Advogado|   M|    SP|   456|
|  5|Mellyssa|   23|      Estudante|   F|    RJ|   463|
+---+--------+-----+---------------+----+------+------+

Command took 2.07 seconds -- by alvarotecno4@gmail.com at 27/02/2023, 14:19:13 on My Cluster
Agrupando por Sexo o numero total de vendas

df.groupBy("Sexo").agg(sum("Vendas")).show()
(2) Spark Jobs
+----+-----------+
|Sexo|sum(Vendas)|
+----+-----------+
|   M|        991|
|   F|        950|
+----+-----------+

Command took 1.58 seconds -- by alvarotecno4@gmail.com at 27/02/2023, 14:19:13 on My Cluster
Estado com mais Vendas

dfagrupado = df.groupBy("Cidade").agg(sum("Vendas")).show()
(2) Spark Jobs
+------+-----------+
|Cidade|sum(Vendas)|
+------+-----------+
|    RJ|        942|
|    MG|        543|
|    SP|        456|
+------+-----------+

Command took 1.09 seconds -- by alvarotecno4@gmail.com at 27/02/2023, 14:19:13 on My Cluster
df.show()
(3) Spark Jobs
+---+--------+-----+---------------+----+------+------+
| ID|    Nome|Idade|      Profissao|Sexo|Cidade|Vendas|
+---+--------+-----+---------------+----+------+------+
|  1|  Alvaro|   31|       Analista|   M|    RJ|   234|
|  2|  Janine|   31|   Dona de Casa|   F|    RJ|   245|
|  3|  Renata|   25|Agente de saude|   F|    MG|   242|
|  4|  Heitor|   11|      Estudante|   M|    MG|   301|
|  5|Mellyssa|   23|      Estudante|   F|    RJ|   463|
|  6|    Jose|   34|       Advogado|   M|    SP|   456|
+---+--------+-----+---------------+----+------+------+

Command took 0.59 seconds -- by alvarotecno4@gmail.com at 27/02/2023, 14:19:13 on My Cluster
df.orderBy("Idade").show()
(1) Spark Jobs
+---+--------+-----+---------------+----+------+------+
| ID|    Nome|Idade|      Profissao|Sexo|Cidade|Vendas|
+---+--------+-----+---------------+----+------+------+
|  4|  Heitor|   11|      Estudante|   M|    MG|   301|
|  5|Mellyssa|   23|      Estudante|   F|    RJ|   463|
|  3|  Renata|   25|Agente de saude|   F|    MG|   242|
|  2|  Janine|   31|   Dona de Casa|   F|    RJ|   245|
|  1|  Alvaro|   31|       Analista|   M|    RJ|   234|
|  6|    Jose|   34|       Advogado|   M|    SP|   456|
+---+--------+-----+---------------+----+------+------+
