import pandas as pd
import uuid
from datetime import datetime
from utils.upsert_to_postgres import upsert_to_postgres


###Setear variables###
input_path = '/Users/leidygomez/Downloads/'

environment = 'production' #staging production
tenant = 'newhaven'

date = datetime.now()
######################


# institutions_institutions
df = pd.read_csv(input_path + 'institutions_program_2024_cargar.csv')
df['uuid'] = df.index.map(lambda x: uuid.uuid4()).map(str)
df['created'] = date
df['modified'] = date 
#df["id"] = range(1, len(df) + 1)
df = df[['uuid', 'created', 'modified', 'campus_code', 'institution_code', 'id', 'year', 'gradetrack_id', 'shift_label_id', 'gender_label_id', 'boolean_vacancies', 'order_recommendation', 'codigo_nacional_1', 'academy_id', 'admission_system_label_id']]

query = f"""
INSERT INTO {tenant}.institutions_program (uuid, created, modified, campus_code, institution_code, id, year, gradetrack_id, shift_label_id, gender_label_id, boolean_vacancies, order_recommendation, codigo_nacional_1, academy_id, admission_system_label_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id)
DO UPDATE SET
    uuid = EXCLUDED.uuid,
    created = EXCLUDED.created,
    modified = EXCLUDED.modified,
    campus_code = EXCLUDED.campus_code,
    institution_code = EXCLUDED.institution_code,
    year = EXCLUDED.year,
    gradetrack_id = EXCLUDED.gradetrack_id,
    shift_label_id = EXCLUDED.shift_label_id,
    gender_label_id = EXCLUDED.gender_label_id,
    boolean_vacancies = EXCLUDED.boolean_vacancies,
    order_recommendation = EXCLUDED.order_recommendation,
    codigo_nacional_1 = EXCLUDED.codigo_nacional_1,
    academy_id = EXCLUDED.academy_id,
    admission_system_label_id = EXCLUDED.admission_system_label_id
RETURNING id;
"""

upsert_to_postgres(df, query, environment, batch_size=500)
