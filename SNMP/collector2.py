from fastapi import FastAPI
from pysnmp.hlapi import *
from datetime import datetime
import threading
import time
import logging
import psycopg2
from concurrent.futures import ThreadPoolExecutor

# Configuración general
app = FastAPI()
COMMUNITY = "public"
INTERVALO_SEGUNDOS = 60

GENERATOR_IPS = {
    "GEN_1": "10.0.24.140",
    "GEN_2": "10.0.24.160",
    "GEN_3": "10.0.24.144",
    "GEN_4": "10.0.24.145",
}

OIDS = {
    "kw_total":   "1.3.6.1.4.1.28634.26.2.8202.0",
    "hours":      "1.3.6.1.4.1.28634.26.2.8206.0",
    "starts":     "1.3.6.1.4.1.28634.26.2.8207.0",
    "rpm":        "1.3.6.1.4.1.28634.26.2.10123.0",
    "voltage_l1": "1.3.6.1.4.1.28634.26.2.9628.0",  
    "voltage_l2": "1.3.6.1.4.1.28634.26.2.9629.0",
    "voltage_l3": "1.3.6.1.4.1.28634.26.2.9630.0",
    "current_l1": "1.3.6.1.4.1.28634.26.2.8198.0",
    "current_l2": "1.3.6.1.4.1.28634.26.2.8199.0",
    "current_l3": "1.3.6.1.4.1.28634.26.2.8200.0",
    "controller_mode": "1.3.6.1.4.1.28634.26.2.9887.0",
}

STATUS_MAP = {
    0: "Apagado",
    1: "Manual",
    2: "Automático",
    3: "Prueba"
}

# Logging
logging.basicConfig(level=logging.INFO)

# Configuración de PostgreSQL
DB_CONFIG = {
    "dbname": "generadores_db",
    "user": "postgres",
    "password": "crypto2025",
    "host": "localhost",
    "port": 5432,
}

def snmp_get(ip: str, oid: str, community: str = COMMUNITY) -> int:
    try:
        iterator = getCmd(
            SnmpEngine(),
            CommunityData(community, mpModel=1),
            UdpTransportTarget((ip, 161), timeout=2, retries=1),
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
        )
        errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
        if errorIndication or errorStatus:
            logging.warning(f"[{ip}] SNMP error: {errorIndication or errorStatus.prettyPrint()}")
            return -1
        for varBind in varBinds:
            return int(varBind[1])
    except Exception as e:
        logging.error(f"Exception for {ip}: {e}")
        return -1

def guardar_en_postgres(data: dict):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO info_generadores (
                timestamp, generator, kw_total, voltage_avg, current_avg,
                hours, starts, rpm, status_GEN
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["timestamp"],
            data["generator"],
            data["kw_total"],
            data["voltage_avg"],
            data["current_avg"],
            data["hours"],
            data["starts"],
            data["rpm"],
            data["status_GEN"],
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error al guardar en PostgreSQL: {e}")

def obtener_datos_generador(name, ip, timestamp):
    # Lecturas crudas de V/I por fase
    v = [snmp_get(ip, OIDS[f"voltage_l{i}"]) for i in (1, 2, 3)]
    c = [snmp_get(ip, OIDS[f"current_l{i}"]) for i in (1, 2, 3)]

    # Promedios (si alguna fase vino mal, -1 como tenías)
    voltage_avg = round(sum(v) / 3) if all(x >= 0 for x in v) else -1
    current_avg = round(sum(c) / 3) if all(x >= 0 for x in c) else -1

    # Lecturas puntuales
    kw      = snmp_get(ip, OIDS["kw_total"])
    hours   = snmp_get(ip, OIDS["hours"])
    starts  = snmp_get(ip, OIDS["starts"])
    rpm     = snmp_get(ip, OIDS["rpm"])
    status_raw = snmp_get(ip, OIDS["controller_mode"])

    # Texto de estado por defecto
    status_text = STATUS_MAP.get(status_raw, f"Desconocido({status_raw})")

    # Heurística de "apagado" o "inaccesible": 
    # - controlador en 0 (Apagado), o
    # - kw <= 0 y rpm <= 0, o
    # - la mayoría de métricas clave vienen en -1 (típico cuando el equipo está apagado y no responde SNMP)
    is_kw_off   = (kw == -1 or kw is None or kw <= 0)
    is_rpm_off  = (rpm == -1 or rpm is None or rpm <= 0)
    is_volt_off = (voltage_avg == -1 or voltage_avg <= 0)
    # cuántos -1/- o None vinieron (para detectar inaccesible)
    invalid_count = sum([
        1 if kw in (-1, None) else 0,
        1 if rpm in (-1, None) else 0,
        1 if voltage_avg == -1 else 0,
        1 if current_avg == -1 else 0,
        1 if hours in (-1, None) else 0,
        1 if starts in (-1, None) else 0,
    ])

    is_off = (
        status_raw == 0 or
        (is_kw_off and is_rpm_off) or
        (is_kw_off and is_volt_off) or
        (is_rpm_off and is_volt_off) or
        invalid_count >= 4  # casi todo vino inválido: tratar como apagado
    )

    if is_off:
        # Forzar ceros visuales cuando está apagado/inaccesible
        kw_final    = 0
        volt_final  = 0
        curr_final  = 0
        rpm_final   = 0
        starts_final = 0
        status_final = "Apagado"
        hours_final  = hours  # lo dejo como viene (totalizador). Si querés 0, lo cambiamos.
    else:
        # Encendido: mantener tu semántica original
        kw_final    = kw
        volt_final  = voltage_avg
        curr_final  = current_avg
        rpm_final   = rpm
        starts_final = starts
        status_final = status_text
        hours_final  = hours

    data = {
        "timestamp": timestamp,
        "generator": name,
        "kw_total": kw_final,
        "voltage_avg": volt_final,
        "current_avg": curr_final,
        "hours": hours_final,
        "starts": starts_final,
        "rpm": rpm_final,
        "status_GEN": status_final,
    }

    guardar_en_postgres(data)
    logging.info(f"Medición {name}: {data}")



def medir_y_guardar():
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with ThreadPoolExecutor(max_workers=4) as executor:
            for name, ip in GENERATOR_IPS.items():
                executor.submit(obtener_datos_generador, name, ip, timestamp)
        time.sleep(INTERVALO_SEGUNDOS)

if __name__ == "__main__":
    import uvicorn
    import socket

    local_ip = socket.gethostbyname(socket.gethostname())
    print(f"API SNMP disponible en: http://{local_ip}:8000")

    hilo = threading.Thread(target=medir_y_guardar, daemon=True)
    hilo.start()

    uvicorn.run("collector2:app", host="0.0.0.0", port=8000, reload=False)
