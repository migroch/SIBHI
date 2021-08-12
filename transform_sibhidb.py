import pandas as pd
import pandas_gbq
import schemas

# Read raw DB data
resources = pd.read_csv('data/SIBHI Survey Results Database - Live School Resource Database.csv')
inputs = pd.read_csv('data/SIBHI Survey Results Database - Live Respondent Input Database.csv')

# Read Codebook data
res_ids = pd.read_csv("data/SIBHI Survey Codebook - Respondnet ID's.csv")
res_values = pd.read_csv('data/SIBHI Survey Codebook - Response values.csv')
var_info = pd.read_csv('data/SIBHI Survey Codebook - Variable info.csv')

# Rename columns
#col_rename_dict = var_info[['Excel variable label','description']].set_index('Excel variable label').to_dict()['description']
col_rename_dict = {'Import?':'Import', 'Resp#':'RespNum', 'SchID.1':'SchID_1', 'DisID.1':'DisID_1' }
resources = resources.rename(columns=col_rename_dict)
inputs = inputs.rename(columns=col_rename_dict)

# Replace numerical values with text values in codebook
res_values = res_values.rename(   # codebook dictionary
    columns={'Excel variable label':'var_label', 'Numeric value':'num_val', 'Response label': 'res_label'}
)
num_val_cols = res_values['var_label'].unique()

def mapper(col, num_val):
    val = res_values.query('var_label==@col and num_val==@num_val').res_label 
    if len(val): return val.iloc[0] 
    else: return num_val 
     
for col in resources.columns:
    if col in num_val_cols:
        resources[col] = resources[col].apply(lambda x: mapper(col, x))

for col in inputs.columns:
    if col in num_val_cols:
        inputs[col] = inputs[col].apply(lambda x: mapper(col, x))

# Convert dtypes
for col in resources.columns: 
    if resources[col].dtype.name=='float64': resources[col]=resources[col].astype('Int64').astype('str') 

for col in inputs.columns: 
    if inputs[col].dtype.name=='float64': inputs[col]=inputs[col].astype('Int64').astype('str') 
        
# Replace null or  invalid inputs        
resources = resources.replace({'99':None, 99:pd.np.nan, '98':'Invalid Input', 98:pd.np.nan, '97':'Invalid Input', 97:pd.np.nan, '96':'Invalid Input', 96:pd.np.nan})
inputs = inputs.replace({'99':None, 99:pd.np.nan, '98':'Invalid Input', 98:pd.np.nan, '97':'Invalid Input', 97:pd.np.nan, '96':'Invalid Input', 96:pd.np.nan})

# Write csv files
resources.to_csv('data/resources.csv', index=False)
inputs.to_csv('data/respondent_inputs.csv', index=False)

# Create bq tables
pandas_gbq.to_gbq(resources, "sibhiDB.resources", if_exists='replace', project_id="sibhi-survey-resources",
                  table_schema=schemas.resources_schema
                  )
pandas_gbq.to_gbq(inputs, "sibhiDB.respondent_inputs", if_exists='replace', project_id="sibhi-survey-resources",
                  table_schema=schemas.inputs_schema
                  )
