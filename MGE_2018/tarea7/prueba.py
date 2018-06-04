from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

sqlContext = SQLContext(sc)

# Leo la base flights.csv
vuelos = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/alex/Documents/Maestria_ciencia_datos/Metodos_gran_escala/datos/flights/flights.csv')

# Elimino aquellas variables que tienen datos "NA", con excepción de "DEPARTURE_DELAY" pues es la variable objetivo, por lo que en este caso elimino los registros para esta variable que tienen "NA".
# Asimismo, selecciono las variables que sulten de interés para predecir la variable "DEPARTURE_DELAY" sin incrementar la dimensionalidad.
vuelos_selec =vuelos.select('MONTH','DAY','DAY_OF_WEEK','FLIGHT_NUMBER','AIR_TIME','DISTANCE','DIVERTED','CANCELLED','DEPARTURE_DELAY').na.drop(subset='DEPARTURE_DELAY')

# Hago el VectorAssembler para definir los features.
vec_ass = VectorAssembler(inputCols=['MONTH','DAY','DAY_OF_WEEK','FLIGHT_NUMBER','AIR_TIME','DISTANCE','DIVERTED','CANCELLED'],outputCol='features')

# Estandarizo las variables.
#std_scaler = StandardScaler(inputCol='features', outputCol='scaled_features')

# Defino un "Decision tree regression" que reciba los features ya estandarizados.
#dtr = DecisionTreeRegressor(featuresCol='scaled_features',labelCol='DEPARTURE_DELAY')
dtr = DecisionTreeRegressor(featuresCol='features',labelCol='DEPARTURE_DELAY')

# Defino el pipeline con el modelo.
#pipeline = Pipeline(stages=[index,vec_ass,std_scaler,dtr])
pipeline = Pipeline(stages=[vec_ass,dtr])

# Divido la base en 70% entrenamiento y 30% prueba.
(entrena,prueba) = vuelos_selec.randomSplit([0.7,0.3])

# Entreno el modelo con los datos de entrenamiento.
modelo = pipeline.fit(entrena)
