import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, regexp_replace

# Para rodar código Spark, sempre devemos criar uma sessão
spark = SparkSession.builder \
  .appName("Projeto1") \
  .getOrCreate()

# Definir manualmente a estrutura dos dados que carregaremos do arquivo JSON
schema = types.StructType([
  types.StructField("nome", types.StringType(), True),
  types.StructField("idade", types.IntegerType(), True),
  types.StructField("email", types.StringType(), True),
  types.StructField("salario", types.IntegerType(), True),
  types.StructField("cidade", types.StringType(), True),
])

df = spark.read.schema(schema).json("data/usuarios.json")

df_sem_email = df.drop("email")

df = df_sem_email.filter(
  (col("idade") > 35) &
  (col("cidade") == 'Natal') &
  (col("salario") < 7000) 
)

df.printSchema()
df.show()

if df.rdd.isEmpty():
  print("Nenhum dado encontrado no arquivo JSON")
else:
  df_clean = df.withColumn("nome", regexp_replace(col("nome"), "@", ""))
  
  sqlite_db_path = os.path.abspath("data/usuarios.db")
  
  sqlite_uri = "jdbc:sqlite://" + sqlite_db_path
  
  properties = {"driver": "org.sqlite.JDBC"}
  
  try:
    spark.read.jdbc(url=sqlite_uri, table='dsa_usuarios', properties=properties)
    write_mode = "append"
  except:
    write_mode = "overwrite"
    
  df_clean.write.jdbc(url=sqlite_uri, table="dsa_usuarios", mode=write_mode, properties=properties)
  
  print(f"Dados gravados no banco de dados SQLite em 'usuarios.db' usando o modo '{write_mode}'")
