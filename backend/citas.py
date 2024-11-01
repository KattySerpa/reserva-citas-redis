from flask import Flask, request, jsonify
from flask_cors import CORS
from redlock import Redlock
import redis
import time
import threading
import pyodbc



app = Flask(__name__)
CORS(app, origins="http://localhost:4200")

""" LOCAL """
redis_client=redis.Redis(host='localhost', port=6379, db=0)

lock_manager = Redlock([
    {
        "host": "localhost",
        "port": 6379,
        "db": 0
    }
])



total_horas = 8

locks = [threading.Lock() for _ in range(total_horas)]
@app.route("/")
def inicializar_horario():
    print(f"INICIO inicializar_horario")
    for i in range(7, 7+total_horas):
        print(f"INICIO RANGE {i}")
        redis_client.set(f"hora_{i}", "libre")
    return jsonify({"message": "Horario inicializado correctamente"})

@app.route("/precalentar")
def precalentar_ubigeo():
    print(f"INICIO precalentar_ubigeo ")
    
    sql_server_connection_string = 'DRIVER={SQL Server};SERVER=LAPTOP-GH8O1ET7;DATABASE=STAGE_DIRESA;UID=kserpa1;PWD=katty'

    try:
        print(f"INICIO try")
        sql_connection = pyodbc.connect(sql_server_connection_string)
        
        #Trabajando con departamentos
        print(f"query departamento")
        sql_query_departamentos = "SELECT distinct Departamento,Codigo_Departamento_Inei FROM Ubigeo_INEI"
        #print(f"INICIO sql_query_departamentos, query {sql_query_departamentos}")
        cursor_departamentos = sql_connection.cursor()
        cursor_departamentos.execute(sql_query_departamentos)
        #print(f"INICIO sql_query_departamentos, cursor_departamentos {cursor_departamentos}")

        print(f"INICIO cursor_departamentos ")
        for row in cursor_departamentos.fetchall():
            #print(f"INICIO cursor_departamentos, row {row}")
            departamento,codigo= row
            #print(f"INICIO departamento,codigo, row {row}")
            redis_client.set(f"Departamento_{codigo}", departamento)

        cursor_departamentos.close()
        
        #Trabajando con provincias
        print(f"query provincia")
        sql_query_provincias = "SELECT distinct Provincia,Codigo_Provincia_Inei FROM Ubigeo_INEI"
        cursor_provincias = sql_connection.cursor()
        cursor_provincias.execute(sql_query_provincias)

        print(f"INICIO cursor_provincias ")
        for row in cursor_provincias.fetchall():
            provincia,codigo= row
            redis_client.set(f"Provincia_{codigo}", provincia)

        cursor_provincias.close()
        
        #Trabajando con distritos
        print(f"query distrito")
        sql_query_distritos = "SELECT distinct Distrito,Codigo_Distrito_Inei FROM Ubigeo_INEI"
        cursor_distritos = sql_connection.cursor()
        cursor_distritos.execute(sql_query_distritos)

        print(f"INICIO cursor_distritos ")
        for row in cursor_distritos.fetchall():
            distrito,codigo= row
            redis_client.set(f"Distrito_{codigo}", distrito)
        
        cursor_distritos.close()
        sql_connection.close()

        #data_precalentada=True
        print(f"precaleteado TRUE")

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Error al precalentar los datos"}), 500


@app.route("/departamentos")
def obtener_departamentos():
    print(f"OBTIENE DEPARTAMENTOS redis")
    matching_keys = redis_client.keys("Departamento_*")
    print(f"INICIO obtener_departamentos {matching_keys}")
    departamentos_array = []
    for key in matching_keys:
        #print(f"INICIO obtener_departamentos {key}")
        prov_json = {
            "llave":str(key).replace("b'","").replace("'",""),
            "valor":str(redis_client.get(key)).replace("b'","").replace("'","")
        }
        #print(f"INICIO obtener_departamentos prov_json {prov_json}")
        departamentos_array.append(prov_json)
    print(f"INICIO obtener_departamentos departamentos_array {departamentos_array}")
    return jsonify(departamentos_array)

@app.route("/provincias/<codigo_departamento>")
def obtener_provincias(codigo_departamento):
    print(f"INICIO obtener_provincias redis  {codigo_departamento}")
    matching_keys = redis_client.keys(f"Provincia_{codigo_departamento}*")
    provincias_array = []
    for key in matching_keys:
        prov_json = {"llave":str(key).replace("b'","").replace("'",""),
                     "valor":str(redis_client.get(key)).replace("b'","").replace("'","")}
        #print(f"INICIO obtener_provincias prov_json {prov_json}")
        provincias_array.append(prov_json)
    return jsonify(provincias_array)

@app.route("/distritos/<codigo_provincia>")
def obtener_distritos(codigo_provincia):
    print(f"obtiene distrito redis")
    matching_keys = redis_client.keys(f"Distrito_{codigo_provincia}*")
    distritos_array = []
    for key in matching_keys:
        prov_json = {"llave":str(key).replace("b'","").replace("'",""),
                     "valor":str(redis_client.get(key)).replace("b'","").replace("'","")}
        distritos_array.append(prov_json)
    return jsonify(distritos_array)

@app.route("/listar_horario")
def listar_horario():
    print(f"listar_horario redis")
    print(f"INICO listar_horario")
    lista = []
    #print(f"RESULT listar_horario {i}")
    for i in range(7, 7+total_horas):
        hora = redis_client.get(f"hora_{i}")
        data = {"hora": i,"estado":hora.decode('utf-8')}
        lista.append(data)
        print(f"RESULT listar_horario lista {lista}")
    return jsonify(lista)

print(f"endpoint listar citas")
@app.route("/listar_citas")
def listar_citas():
    print(f"listar citas redis")
    lista = []
    matching_keys = redis_client.keys("cita:*")

# Iterate through the matching keys and get the corresponding values
    print(f"listar_citas redis getAll")
    for key in matching_keys:
        value = redis_client.hgetall(key)
        # Decode bytes to strings in the value dictionary
        decoded_value = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}

        data = {"key": key.decode('utf-8'), "value": decoded_value}
        lista.append(data)
    
    return jsonify(lista)

@app.route("/proceso_cita/<hora_seleccionada>")
def procesa_reservacion_hora(hora_seleccionada):
    print(f"inicio procesa cita/selec horario")
    if int(hora_seleccionada) > 14 or int(hora_seleccionada) < 7:
        return "El horario de atencion para citas es de 7 AM hasta 2 PM"

    if not locks[int(hora_seleccionada) - 7].acquire(blocking=False):
        return jsonify({"message": "Hora seleccionado actualmente en proceso de reserva por otro contribuyente"})

    current_status = redis_client.get(f"hora_{hora_seleccionada}").decode()
    
    print(f"estado {current_status}")
    if current_status in ('bloqueado','reservado'):
        locks[int(hora_seleccionada) - 7].release()
        return jsonify({"message": "Hora no disponible"})

    try:
        redis_client.set(f"hora_{hora_seleccionada}", "bloqueado")
        threading.Thread(target=desbloquear_hora, args=(int(hora_seleccionada),)).start()
        return jsonify({"message": f"Hora {hora_seleccionada} bloqueada mientras se procesa la reservación de cita"})
    finally:
        locks[int(hora_seleccionada) - 7].release()

def desbloquear_hora(hora_number):
    print(f"desbloquear hora {hora_number}")
    time.sleep(10)# 10 segundos para desbloquear
    current_status = redis_client.get(f"hora_{hora_number}").decode()
    if current_status != 'reservado':
        locks[hora_number - 7].acquire()
        redis_client.set(f"hora_{hora_number}", "libre")
        locks[hora_number - 7].release()

@app.route("/agregar_cita/<hora_seleccionada>",methods=['POST'])
def agregar_cita(hora_seleccionada):
    current_status = redis_client.get(f"hora_{hora_seleccionada}").decode()
    print(f"agregar_cita {current_status}")
    if current_status == 'bloqueado':
        redis_client.incr('contador_citas')
        
        redis_client.set(f"hora_{hora_seleccionada}", "reservado")

        data = request.json
        user_key = "cita:"+redis_client.get('contador_citas').decode("utf-8")

        # Guarda los datos en una hash
        redis_client.hset(user_key, "nombre", data['nombre'])
        redis_client.hset(user_key, "apellido_paterno", data['apellido_paterno'])
        redis_client.hset(user_key, "apellido_materno", data['apellido_materno'])
        redis_client.hset(user_key, "departamento", data['departamento'])
        redis_client.hset(user_key, "provincia", data['provincia'])
        redis_client.hset(user_key, "distrito", data['distrito'])
        redis_client.hset(user_key, "tramite", data['tramite'])
        redis_client.hset(user_key, "hora_reservada", hora_seleccionada)

        print(f"redis client {redis_client}")

        return jsonify({"message":f"Hora {hora_seleccionada} reservada con exito"})
    return jsonify({"message":"No se ha reservado, verificar el horario"})

if __name__ == "__main__":
    print(f"##### Inicio main ######")
    precalentar_ubigeo()

    """ojo agregare -probando"""
    with app.app_context():
        inicializar_horario()
        #departamentos = obtener_departamentos() 
    app.run(debug=True)