import pandas as pd
import uuid
from datetime import datetime
from utils.upsert_to_postgres import upsert_to_postgres


###Setear variables###
input_path = '/Users/leidygomez/ConsiliumBots Dropbox/Leidy Gomez/Explorador_CB/Explorador_Copenhague/Outputs/'

environment = 'staging' #staging production
tenant = 'dnk'

date = datetime.now()
######################


# institutions_institutions
df = pd.read_csv(input_path + 'institutions_campus.csv')
df['uuid'] = df.index.map(lambda x: uuid.uuid4()).map(str)
df['created'] = date
df['modified'] = date 
df = df[['uuid', 'created', 'modified', 'campus_code', 'institution_code', 'campus_name', 'campus_short_name', 'internet', 'need_verification', 'included', 'profile_type', 'closed', 'primary_national_code', 'secondary_national_code', 'sector_label_id']]

query = f"""
INSERT INTO {tenant}.institutions_campus (uuid, created, modified, campus_code, institution_code, campus_name, campus_short_name, internet, need_verification, included, profile_type, closed, primary_national_code, secondary_national_code, sector_label_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (campus_code)
DO UPDATE SET
    uuid = EXCLUDED.uuid,
    created = EXCLUDED.created,
    modified = EXCLUDED.modified,
    institution_code = EXCLUDED.institution_code,
    campus_name = EXCLUDED.campus_name,
    campus_short_name = EXCLUDED.campus_short_name,
    internet = EXCLUDED.internet,
    need_verification = EXCLUDED.need_verification,
    included = EXCLUDED.included,
    profile_type = EXCLUDED.profile_type,
    closed = EXCLUDED.closed,
    primary_national_code = EXCLUDED.primary_national_code,
    secondary_national_code = EXCLUDED.secondary_national_code,
    sector_label_id = EXCLUDED.sector_label_id
RETURNING id;
"""

upsert_to_postgres(df, query, environment, batch_size=500)
