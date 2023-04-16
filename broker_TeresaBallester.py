"""
Teresa Ballester Navarro - Programacion Paralela 2022/23

Broker:
------
Los usuarios que se conectan, pueden enviar y recibir mensajes en el topic clients. 
Comprueba, en primer lugar, que puedes conectarte al broker y enviar y recibir mensajes
"""

import sys
from paho.mqtt.client import Client

def on_message(mqtt_cliente, userdata, msg):#Función para recibir un mensaje y el broker lo publica
    print('......................')
    print('topic: %s' % msg.topic)
    print('payload: %s' % msg.payload)
    print('qos: %s' % msg.qos)
    mqtt_cliente.publish('clients/test', msg.payload)
    
def on_connect(mqtt_cliente, userdata): #Función para que el cliente se subscriba al topic
    print('connected: %s' % mqtt_cliente._client_id)
    mqtt_cliente.subscribe(topic)
    
def main(broker, topic):
    mqtt_cliente = Client()
    mqtt_cliente.on_message = on_message
    mqtt_cliente.on_connect = on_connect
    mqtt_cliente.connect(broker) #Me conecto al servidor mqtt
    mqtt_cliente.subscribe(topic) #El subscriptor se subscribe al topic
    mqtt_cliente.loop_forever() #Necesario para poder recibir mensajes
    
if __name__ == '__main__':
    if len(sys.argv)<3:
        print(f"Usage: {sys.argv[0]} broker topic") 
        sys.exit(1)
    broker = sys.argv[1]
    topic = sys.argv[2]
    main(broker, topic)