"""
Teresa Ballester Navarro

En el topic numbers se están publicando constantemente números,los hay enteros y los hay
reales.Escribe el código de un cliente mqtt que lea este topic y que realice tareas con los
números leídos,por ejemplo, separar los enteros y reales,calcular la frecuencia de cada uno
de ellos, estudiar propiedades (como ser o no primo) en los enteros, etc.
"""
import sys
from paho.mqtt.client import Client
from multiprocessing import Process, Manager 
from time import sleep
import random

NUMBERS = 'numbers'
CLIENTS = 'clients'
TIMER_STOP = f'{CLIENTS}/timerstop' 
HUMIDITY = 'humidity'

def is_prime(n): 
    """
    Función que devulece True si n es primo y False en caso contrario.
    """
    p = 2
    while p*p < n and n % p != 0: 
        p += 1
    return p*p > n

def timer(time, data): 
    cliente = Client()
    cliente.connect(data['broker']) 
    msg = f'timer working. timeout: {time}'
    print(msg) 
    cliente.publish(TIMER_STOP, msg) 
    sleep(time)
    msg = f'timer working. timeout: {time}' 
    cliente.publish(TIMER_STOP, msg) 
    print('timer end working') 
    cliente.disconnect()

def on_message(cliente, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}") 
    try:
        #if is_prime(int(msg.payload)):
        if int(msg.payload) % 2 == 0: 
            worker = Process(target=timer,args=(random.random()*20, data))
            worker.start() 
    except ValueError as e:
        print(e)
        pass

def on_log(cliente, userdata, level, string):
    print("LOG", userdata, level, string)

def main(broker):
    data = {'client':None,'broker': broker}
    cliente = Client(client_id="combine_numbers", userdata=data) 
    data['client'] = cliente
    cliente.enable_logger()
    cliente.on_message = on_message
    cliente.on_log = on_log
    cliente.connect(broker)
    cliente.subscribe(NUMBERS)
    cliente.loop_forever()

if __name__ == "__main__": 
    if len(sys.argv) < 2: 
        print(f"Usage: {sys.argv[0]} broker") 
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)