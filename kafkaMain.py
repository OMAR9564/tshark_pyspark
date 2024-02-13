from confluent_kafka import Producer
import json, re
import time
import os

bootstrap_servers = 'localhost:9092'
topic = 'jsonSpark_logs'

def fixJSON(jsonStr):
    if not jsonStr.endswith(']') and not jsonStr=='':
        jsonStr += ']'

    return (jsonStr)

#  Kafka producer olsutru
producer = Producer({'bootstrap.servers': bootstrap_servers})

json_dosyasi_yolu = 'data/http_traffic.json'

last_record_count = 0
last_modified = 0
while True:
    current_record_count = 0

    with open(json_dosyasi_yolu, 'r+') as dosya:

        #emtpy josn file
        try:
            veri = json.load(dosya)
            current_record_count = len(veri)
        except json.JSONDecodeError as e:
            if "Expecting ','" or "Expecting property" or "Extra data" in str(e):
                try:
                    json_str = dosya.read()
                    veri = fixJSON(json_str)
                    current_record_count = len(veri)
                except json.JSONDecodeError as e:
                    print("Hata: JSON dosyası düzeltilirken bir sorun oluştu. Hata:", str(e))
            if "Expecting value" in str(e):
                print(str(e))
            time.sleep(2)
            continue


        if current_record_count > last_record_count:
            producer.produce(topic, value=json.dumps(veri).encode('utf-8'))
            producer.flush()
            print("Yeni kayıtlar Kafka'ya gönderildi.")
            last_record_count = current_record_count

        time.sleep(5)