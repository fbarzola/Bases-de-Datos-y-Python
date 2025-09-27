import psycopg2
from psycopg2.extras import execute_batch

LOCAL = dict(
    host="localhost",          # cámbialo si tu Postgres local está en otro servidor
    dbname="generadores_db",
    user="postgres",
    password="crypto2025",
    port=5432,
)

REMOTE = dict(
    host="cryptopatagonia.c09qa6uycr9v.us-east-1.rds.amazonaws.com",
    dbname="cryptopatagonia",
    user="postgres",
    password="kg8SGNdQxekBpKo9sWlw",
    port=5432,
    sslmode="verify-full",
    sslrootcert=r"C:\WINDOWS\system32\rds-combined-ca-bundle.pem",
)

DEST_SCHEMA = "public"
DEST_TABLE  = "info_generadores_web"
BATCH_SIZE  = 5000

def connect_local():
    print("Conectando a Postgres LOCAL...")
    conn = psycopg2.connect(
        host=LOCAL["host"],
        dbname=LOCAL["dbname"],
        user=LOCAL["user"],
        password=LOCAL["password"],
        port=LOCAL["port"],
        connect_timeout=8,
        options='-c statement_timeout=30000'
    )
    print("LOCAL conectado.")
    return conn

def connect_remote():
    print("Conectando a Postgres REMOTO (RDS)...")
    conn = psycopg2.connect(
        host=REMOTE["host"],
        dbname=REMOTE["dbname"],
        user=REMOTE["user"],
        password=REMOTE["password"],
        port=REMOTE["port"],
        sslmode=REMOTE["sslmode"],
        sslrootcert=REMOTE["sslrootcert"],
        connect_timeout=8,
        options='-c statement_timeout=30000'
    )
    print("REMOTO conectado.")
    return conn

def ensure_table_remote(cur):
    print("Creando tabla destino si no existe...")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {DEST_SCHEMA}.{DEST_TABLE} (
        id            INTEGER PRIMARY KEY,
        timestamp     TIMESTAMP NOT NULL,
        generator     TEXT NOT NULL,
        kw_total      DOUBLE PRECISION,
        voltage_avg   DOUBLE PRECISION,
        current_avg   DOUBLE PRECISION,
        hours         INTEGER,
        starts        INTEGER,
        rpm           INTEGER,
        status_gen    TEXT
    );
    """)
    cur.execute(f"""
    CREATE INDEX IF NOT EXISTS idx_{DEST_TABLE}_ts
    ON {DEST_SCHEMA}.{DEST_TABLE} (timestamp);
    """)
    print("Tabla/indice OK.")

def get_max_id_remote(cur):
    cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {DEST_SCHEMA}.{DEST_TABLE};")
    (mx,) = cur.fetchone()
    return mx or 0

def main():
    try:
        local_conn = connect_local()
    except Exception as e:
        print("Error conectando a LOCAL:", str(e))
        return

    try:
        remote_conn = connect_remote()
    except Exception as e:
        print("Error conectando a REMOTO:", str(e))
        return

    local_cur  = local_conn.cursor()
    remote_cur = remote_conn.cursor()

    try:
        ensure_table_remote(remote_cur)
        remote_conn.commit()

        max_remote_id = get_max_id_remote(remote_cur)
        print("Max id en remoto:", max_remote_id)

        local_cur.execute("SELECT COUNT(*) FROM info_generadores WHERE id > %s;", (max_remote_id,))
        (pending_count,) = local_cur.fetchone()
        print("Registros pendientes de enviar:", pending_count)

        if pending_count == 0:
            print("Nada para sincronizar. Fin.")
        else:
            offset = 0
            enviados = 0
            while True:
                print("Leyendo batch desde local. Offset:", offset)
                local_cur.execute("""
                    SELECT id, timestamp, generator, kw_total, voltage_avg, current_avg, hours, starts, rpm, status_gen
                    FROM info_generadores
                    WHERE id > %s
                    ORDER BY id
                    LIMIT %s OFFSET %s;
                """, (max_remote_id, BATCH_SIZE, offset))
                rows = local_cur.fetchall()
                if not rows:
                    break

                print("Upsert de filas:", len(rows))
                execute_batch(remote_cur, f"""
                    INSERT INTO {DEST_SCHEMA}.{DEST_TABLE}
                    (id, timestamp, generator, kw_total, voltage_avg, current_avg, hours, starts, rpm, status_gen)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET
                        timestamp   = EXCLUDED.timestamp,
                        generator   = EXCLUDED.generator,
                        kw_total    = EXCLUDED.kw_total,
                        voltage_avg = EXCLUDED.voltage_avg,
                        current_avg = EXCLUDED.current_avg,
                        hours       = EXCLUDED.hours,
                        starts      = EXCLUDED.starts,
                        rpm         = EXCLUDED.rpm,
                        status_gen  = EXCLUDED.status_gen;
                """, rows, page_size=2000)

                remote_conn.commit()
                enviados += len(rows)
                print("Batch confirmado. Total enviados:", enviados)
                offset += BATCH_SIZE

        print("Sincronizacion completada.")

    except Exception as e:
        remote_conn.rollback()
        print("Error durante la sincronizacion:", str(e))
    finally:
        local_cur.close()
        remote_cur.close()
        local_conn.close()
        remote_conn.close()

if __name__ == "__main__":
    main()

