import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2

#Funcion para conectarse a la base de datos
def conect_bd(database, environment):
    if environment == 'staging':
        host = "staging.cluster-cuasuehzhaay.us-east-1.rds.amazonaws.com"
    elif environment == 'production':
        host = "production.cluster-cuasuehzhaay.us-east-1.rds.amazonaws.com"  
    conn = psycopg2.connect(
        database = database,
        user = os.getenv("db_user"),
        password = os.getenv("db_password"),
        port = os.getenv("db_port"),
        host = host
    )
    return conn
