# Tarea 3 - Procesamiento de Datos con Apache Spark

Universidad Nacional Abierta y a Distancia (UNAD)
Curso: Big Data - Ingeniería de Sistemas

## Descripción del proyecto

Este proyecto analiza solicitudes ciudadanas del sistema
NYC 311 de Nueva York mediante Apache Spark y Python.

Se desarrollaron dos componentes:
- Procesamiento por lotes (Batch).
- Procesamiento en tiempo real con Kafka y Spark Streaming.

## Tecnologías utilizadas

- Ubuntu Linux
- Python 3.10
- Apache Hadoop HDFS
- Apache Spark 3.5.8
- Apache Kafka
- PySpark
- Matplotlib

## Conjunto de datos

Fuente: NYC Open Data - 311 Service Requests.
Periodo: enero a diciembre de 2024.
Se utiliza una muestra filtrada del conjunto original.

Archivo: 311_2024.csv
Ubicación en HDFS: /Tarea3/311_2024.csv

## Archivos del proyecto

- batch_311.py: limpieza, transformación y análisis exploratorio.
- kafka_producer_311.py: generación de mensajes simulados.
- spark_streaming_311.py: procesamiento de datos en tiempo real.

## Ejecución del procesamiento batch

Con Hadoop HDFS y Spark configurados:

spark-submit batch_311.py

El programa genera estadísticas y gráficas sobre:
- Tipos de solicitudes más frecuentes.
- Cantidad de solicitudes por mes.
- Distribución por distrito.
- Tiempos de resolución por entidad.

## Ejecución del procesamiento en tiempo real

Con Apache Kafka configurado y en funcionamiento:

1. Ejecutar el productor:
python3 kafka_producer_311.py

2. En otra terminal, ejecutar el consumidor:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_311.py

## Resultados

El procesamiento batch genera tablas y gráficas.
Spark Streaming permite analizar mensajes simulados
y agrupar las solicitudes por ventanas de tiempo.

## Autor

Alfonso López Marín
Universidad Nacional Abierta y a Distancia - UNAD
2026
