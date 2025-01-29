from psycopg2.extras import execute_batch
from utils.db_connection import conect_bd


def upsert_to_postgres(df, query, environment, batch_size=500):
    # Crear la conexión
    conn = conect_bd('core', environment)
    cursor = conn.cursor()

    try:
        with conn:
            with cursor:
                # Iterar sobre los datos en batches
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    # Convertir el batch en una lista de tuplas
                    data_to_insert = list(batch.itertuples(index=False, name=None))
                    
                    # Ejecutar el batch
                    execute_batch(cursor, query, data_to_insert)
        print("Carga completada con éxito.")
    except Exception as e:
        conn.rollback()
        print(f"Error durante la carga: {e}")
    finally:
        cursor.close()
        conn.close()
