import pandas as pd
import uuid
from utils.upsert_to_postgres import upsert_to_postgres


###Setear variables###
input_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Copenhague/Outputs/'

environment = 'staging' #staging production
tenant = 'dnk'
######################


# institutions_institutions
df = pd.read_csv(input_path + 'institutions_institutions.csv')
df['uuid'] = df.index.map(lambda x: uuid.uuid4()).map(str)
df = df[['uuid', 'institution_code', 'institution_name', 'institution_short_name']]

query = f"""
INSERT INTO {tenant}.institutions_institutions (uuid, institution_code, institution_name, institution_short_name)
VALUES (%s, %s, %s, %s)
ON CONFLICT (institution_code)
DO UPDATE SET
    uuid = EXCLUDED.uuid,
    institution_name = EXCLUDED.institution_name,
    institution_short_name = EXCLUDED.institution_short_name
RETURNING id;
"""

upsert_to_postgres(df, query, environment, batch_size=500)

