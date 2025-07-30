from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col
import time


spark = SparkSession.builder.appName("WordCountComparison").getOrCreate()
sc = spark.sparkContext


input_path = "/Users/yamelmiranda/Desktop/Cuatrimestre 11/Programación paralela y distribuida/actividad semana 13/requirements.txt" 

# ---------- Word Count con RDD ----------
start_rdd = time.time()

rdd = sc.textFile(input_path)
words = rdd.flatMap(lambda line: line.split())
word_pairs = words.map(lambda w: (w.lower(), 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

word_counts.saveAsTextFile("salida/rdd_wordcount")

end_rdd = time.time()
time_rdd = end_rdd - start_rdd
print(f"Tiempo Word Count con RDD: {time_rdd:.2f} segundos")

# ---------- Word Count con DataFrame ----------
start_df = time.time()

df = spark.read.text(input_path)
words_df = df.select(explode(split(col("value"), "\s+")).alias("word")) \
             .select(lower(col("word")).alias("word"))

word_counts_df = words_df.groupBy("word").count().orderBy(col("count").desc())

# Guardar resultado en CSV
word_counts_df.write.mode("overwrite").csv("salida/df_wordcount", header=True)

end_df = time.time()
time_df = end_df - start_df
print(f"Tiempo Word Count con DataFrame: {time_df:.2f} segundos")

# ---------- Speedup ----------
if time_df > 0:
    speedup = time_rdd / time_df
    print(f"Speedup (RDD / DataFrame): {speedup:.2f}")
else:
    print("Error: tiempo DataFrame cero.")

# Cerrar Spark al finalizar
spark.stop()

