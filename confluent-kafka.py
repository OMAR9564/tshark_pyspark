import re
from confluent_kafka import Consumer, KafkaException
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("KafkaIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

bootstrap_servers = 'localhost:9092'
group_id = 'my_consumer_group'
topic = 'jsonSpark_logs'

conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])

schemaStructType = []

try:

    def replace_values(data, replacements):
        data_copy = json.loads(json.dumps(data))
        data_string = json.dumps(data)
        for old_char, new_char in replacements.items():
            data_string = re.sub(old_char, new_char, data_string)
        data_json = json.loads(data_string)
        return data_json

    from pyspark.sql.types import StructType, StructField, ArrayType



    def flatten(df):
        complex_fields = dict([(field.name, field.dataType)

        for field in df.schema.fields
            if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        while len(complex_fields) != 0:
            col_name = list(complex_fields.keys())[0]
            print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))
            if (type(complex_fields[col_name]) == StructType):
                expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in complex_fields[col_name]]]
                df = df.select("*", *expanded).drop(col_name)

            elif (type(complex_fields[col_name]) == ArrayType):
                df = df.withColumn(col_name, explode_outer(col_name))

            complex_fields = dict([(field.name, field.dataType)

                for field in df.schema.fields
                    if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        return df


    def flatten_schema(schema, prefix=""):
        stringHeaderLongShort = []

        for field in schema.fields:
            field_name = prefix + field.name
            if isinstance(field.dataType, StructType):
                stringHeaderLongShort.append(field.name)
                stringHeaderLongShort.extend(flatten_schema(field.dataType, prefix=field_name + "."))
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                stringHeaderLongShort.append(field.name)
                stringHeaderLongShort.extend(flatten_schema(field.dataType.elementType, prefix=field_name + "."))
            else:
                last_part_fieldName = field.name.split('.')[-1]
                if prefix != "":
                    stringHeaderLongShort.append(field.name)
        return stringHeaderLongShort

    while True:
        msg = consumer.poll(1.0)
        schemaStructType = []

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                # Diğer hatalar
                raise KafkaException(msg.error())
        else:
            json_data = json.loads(msg.value().decode('utf-8'))

            kafka_params = {
                'kafka.bootstrap.servers': bootstrap_servers,
                'kafka.group.id': group_id,
                'subscribe': topic,
                'kafka.value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer'
            }

            json_data = json.loads(msg.value().decode('utf-8'))
            json_data = json.dumps(json_data, indent=2)
            df = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize([json_data]))


            fixed_json_data = {}
            headers_colmuns = flatten_schema(df.schema)
            fixed_header_colmuns = [header.replace(".", "@") for header in headers_colmuns]
            json_data = json.loads(json_data)
            replacement = {}
            for headers_colmun, fixed_header_colmun in zip(headers_colmuns, fixed_header_colmuns):
                replacement[headers_colmun] = fixed_header_colmun
            new_json_data = replace_values(json_data, replacement)
            new_json_data = json.dumps(new_json_data, indent=2)
            df2 = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize([new_json_data]))


            df2 = flatten(df2)


            df.printSchema()

            df2.show(truncate=True, n=10, vertical=False)

            rows = df2.collect()
            result = []
            for i, row in enumerate(rows):
                # Satırın değerlerini birleştirip bir listeye ekleyelim
                row_values = [str(value) for value in row]
                result.append((i, " ".join(row_values)))
                if i != len(result) - 1:
                    result.append(",")
            # Elde edilen sonucu yazdıralım
            idx = 0
            for item in result:
                if idx != len(result) - 1:
                    print(f"{item}, ")
                    idx += 1
                else:
                    print(item)


except KeyboardInterrupt:
    pass

finally:
    consumer.close()
