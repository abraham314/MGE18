# word_count.py
import pprint

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

# Convertimos los RDD en el DStream a DataFrame para ejecutar alguna funcion de SQL
def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        row_rdd = rdd.map(lambda w: Row(word=w))
        words_data_frame = spark.createDataFrame(row_rdd)

        # Creates a temporary view using the DataFrame.
        words_data_frame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        word_counts_data_frame = \
            spark.sql("select word, count(*) as total from words group by word")
        word_counts_data_frame.show()
    except:
        pass


def count_words():
    # Definimos el numero de cores que necesitamos, como este ejemplo corre local
    # ocupamos el local[n] que permite dedicar n numero de hilos al input DStream,
    # al receiver y al procesamiento.
    sc = SparkContext("local[2]")
    # definimos el intervalo de tiempo de stream (cada segundo generaremos)
    # batches de DStream
    streaming_context = StreamingContext(sc, 1)

    # Creamos un input DStream de tipo socket -basico- con el endpoint
    # localhost = 127.0.0.1 en el puerto 9999
    lines = streaming_context.socketTextStream("localhost", 9999)

    # de cada DStream que obtenemos queremos dividir en palabras para poder contarlas
    words = lines.flatMap(lambda line: line.split(" "))
    print(type(words))

    # Procesamos los DStream haciendo un conteo al estilo map/reduce
    pairs = words.map(lambda word: (word, 1))
    word_counts = pairs.reduceByKey(lambda x, y: x + y)

    # mostramos los primeros 10 elementos de cada RDD generado en el DStream en consola
    word_counts.pprint()

    #### Hasta este punto realmente no hemos ejecutado nada, solo hemos
    # configurado todo lo que necesitamos para recibir y procesar el stream de datos
    # para iniciar el procesamiento del streaming necesitamos hacer el llamada al
    # metodo start() del StreamingContext
    
    words.foreachRDD(process)
    streaming_context.start()
    streaming_context.awaitTermination()


if __name__ == "__main__":
    count_words()
