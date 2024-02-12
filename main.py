from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq

credentials = service_account.Credentials.from_service_account_file('keen-button-410620-d9e1076f2fbd.json')

project_id = 'keen-button-410620'
client = bigquery.Client(credentials= credentials,project=project_id)

query = client.query("""
   SELECT * FROM `keen-button-410620.nypd_arrest_data.nypd-arrestdata-historic-2018-to-2023`
 """)

result_df = query.to_dataframe()
print(result_df.head(5))

category_mapping = {
    'F': 'Felony',
    'M': 'Misdemeanor',
    'I': 'Infraction',
    'V': 'Violation'
}

# Replace values in the LAW_CAT_CD column using the defined mapping
result_df['LAW_CAT_CD'] = result_df['LAW_CAT_CD'].replace(category_mapping)

# Replace blank values in the LAW_CAT_CD column with 'Unclassified'
result_df['LAW_CAT_CD'].fillna('Unclassified', inplace=True)

# Remove rows where LAW_CAT_CD is '9'
result_df= result_df[result_df['LAW_CAT_CD'] != '9']

boro_mapping = {
    'B': 'Bronx',
    'S': 'Staten Island',
    'K': 'Brooklyn',
    'M': 'Manhattan',
    'Q': 'Queens'
}

# Replace values in the ARREST_BORO column using the defined mapping
result_df['ARREST_BORO'] = result_df['ARREST_BORO'].replace(boro_mapping)


# Define a function to determine jurisdiction based on jurisdiction codes
def determine_jurisdiction(code):
    if code in [0, 1, 2]:
        return 'NYPD'
    elif code >= 3:
        return 'Non-NYPD'
    else:
        return 'Unknown'  # You can customize this behavior based on your requirements

# Add a new column 'Jurisdiction' based on jurisdiction codes
result_df['Jurisdiction'] = result_df['JURISDICTION_CODE'].apply(determine_jurisdiction)

# Define a mapping for replacing values in the PERP_SEX column
sex_mapping = {
    'F': 'Female',
    'M': 'Male'
}

# Replace values in the PERP_SEX column using the defined mapping
result_df['PERP_SEX'] = result_df['PERP_SEX'].replace(sex_mapping)

#format the date
result_df['ARREST_DATE'] = pd.to_datetime(result_df['ARREST_DATE'])

# Format the 'ARREST_DATE' column to 'YYYY-MM-DD' format
result_df['ARREST_DATE'] = result_df['ARREST_DATE'].dt.strftime('%Y-%m-%d')


result_df['ARREST_DATE'] = pd.to_datetime(result_df['ARREST_DATE'], format='%Y-%m-%d')
print(result_df.info())

print(result_df.head(5))

 #Define the BigQuery destination table (replace with your project ID, dataset ID, and table name)
destination_table = 'keen-button-410620.nypd_arrest_data.nypd-arrestdata-historic-2018-to-2023_processed'

# Write the DataFrame to BigQuery
pandas_gbq.to_gbq(result_df, destination_table, project_id=project_id, if_exists='replace', credentials=credentials)

print("DataFrame successfully written to BigQuery.")

