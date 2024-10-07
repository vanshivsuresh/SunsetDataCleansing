import pandas as pd
import numpy as np
from unidecode import unidecode
import phonenumbers
import re
from phonenumbers.phonenumberutil import region_code_for_number 
from phonenumbers import NumberParseException
from email_validator import validate_email as ev, EmailNotValidError
# from email_validator import validate_email, EmailNotValidError
from email.utils import parseaddr
from validate_email_address import validate_email as validate_email_lib

 
#### Input Output JOE_CUSTOMERS FILES
input_file = "C:\\Users\\Lenovo\\Downloads\\JOE_USTOMERS_ 2024-07-31T1718 (4).csv"
output_file = "C:\\Users\\Lenovo\\Downloads\\SunSet Cleansing Data File\\ShortJoe_Case_30_09.csv"


"""### Input Output SUSHISAMBA FILES
input_file = "C:\\Users\\Lenovo\\Downloads\\SUSHISAMBA_CUST_EXPORTS (1)\\SUSHISAMBA_CUST_EXPORTS.csv"
output_file = "C:\\Users\\Lenovo\\Downloads\\SUSHISAMBA_CUST_EXPORTS (1)\\SUSHISAMBA_Final_26_09.csv"""
#input_file = "C:\\Users\\Lenovo\\Downloads\\ShortJoe_Case (1).csv"

chunk = pd.read_csv(input_file)


chunk['Venue Parent Group Guest Profile Creation Date'] = pd.to_datetime(chunk['Venue Parent Group Guest Profile Creation Date'],infer_datetime_format=True, errors='coerce')

print("data type of creationd ate is =========",chunk['Venue Parent Group Guest Profile Creation Date'])

## just for formatting    

muhammed_variations = ['Muhammed', 'Muhammad', 'Mohammad', 'Mohammed','Mohamed','Mohamad','Mhd','Muhamed']

chunk['Venue Parent Group Guest Guest Email'] = chunk['Venue Parent Group Guest Guest Email'].str.lower().str.strip()

chunk['Venue Parent Group Guest Phone Number'] = chunk['Venue Parent Group Guest Phone Number']

initial_count = len(chunk)
print(f"Initial row count: {initial_count}")



###### Initial deduplicating
"""df_no_duplicates = df.drop_duplicates(subset=["Valid Emails", "MOBILE_UNQ"], keep='first')"""
chunk = chunk.sort_values(by=['Venue Parent Group Guest Profile Creation Date'], ascending=[False])
chunk = chunk.drop_duplicates(subset=['Venue Parent Group Guest Phone Number', 'Venue Parent Group Guest Guest Email'], keep='first')
chunk.reset_index(drop=True, inplace=True)
# chunk = chunk.drop_duplicates(subset=["Venue Parent Group Guest Phone Number", "Venue Parent Group Guest Guest Email"], keep='first')
print(f"deduplications Initial row count:", len(chunk))




###### update salutations 

def update_salutation(row):
    if row['Venue Parent Group Guest Full Name'].startswith(('Ms ', 'Ms. ', 'Ms.')) and pd.isna(row['Venue Parent Group Guest Salutation']):
        return 'Ms.'
    elif row['Venue Parent Group Guest Full Name'].startswith(('Mr. ', 'Mr ', 'Mr.')) and pd.isna(row['Venue Parent Group Guest Salutation']):
        return 'Mr.'
    elif row['Venue Parent Group Guest Full Name'].startswith(('Mrs. ', 'Mrs.', 'Mrs ')) and pd.isna(row['Venue Parent Group Guest Salutation']):
        return 'Mrs.'
    elif row['Venue Parent Group Guest Full Name'].startswith(('Miss. ', 'Miss ', 'Miss.')) and pd.isna(row['Venue Parent Group Guest Salutation']):
        return 'Ms.'
    elif row['Venue Parent Group Guest Full Name'].startswith(('Dr. ', 'Dr.', 'Dr ')) and pd.isna(row['Venue Parent Group Guest Salutation']):
        return 'Dr.'
    else:
        return row['Venue Parent Group Guest Salutation']
    


## steps to update saluations if name start with muhammed.

def handle_muhammed(row):
    full_name = row['Venue Parent Group Guest Full Name']
    salutation = row['Venue Parent Group Guest Salutation']
    
    for name in muhammed_variations:
        if name.lower() in full_name.lower():
            return 'Mr.'
    
    return salutation

# Update Salutation
chunk['Venue Parent Group Guest Salutation'] = chunk.apply(update_salutation, axis=1)
chunk['Venue Parent Group Guest Salutation'] = chunk.apply(handle_muhammed, axis=1)
chunk['Venue Parent Group Guest Salutation'] = chunk['Venue Parent Group Guest Salutation'].replace({
    'Ms': 'Ms.',
    'Mr': 'Mr.',
    'Mrs': 'Mrs.',
    'Miss': 'Mrs.',
    'Dr': 'Dr.'
})


# Add the Gender column
chunk['Gender'] = None
chunk.loc[chunk['Venue Parent Group Guest Salutation'].isin(['Mrs.', 'Ms.']), 'Gender'] = 'Female'
chunk.loc[chunk['Venue Parent Group Guest Salutation'] == 'Mr.', 'Gender'] = 'Male'


### Email validator
def validate_email(email):
    if not email or pd.isna(email):
        return "Invalid"
    
    if (re.search(r'[&\'",:;!+=\/()<>]', email) or
        re.search(r'^[@\.\-_]', email) or
        re.search(r'[@\.\-_]$', email) or
        '..' in email or
        '@@' in email or
        re.search(r'@\.|\.\@', email) or
        email.endswith(('.cm', '.co', '.or', '.ne')) or
        not re.match(r'^.+@.+\..+$', email) or
        not re.match(r'.+@.+\..+', email)):

        return "Invalid"

    return "Valid"


# # Apply the validation logic to the dataframe
chunk['Invalid Emails'] = chunk['Venue Parent Group Guest Guest Email'].apply(lambda x: x if validate_email(x) == "Invalid" else None)
chunk['Valid Emails'] = chunk['Venue Parent Group Guest Guest Email'].apply(lambda x: x if validate_email(x) == "Valid" else None)


print("total emails is ===",len(chunk['Valid Emails']))


### cleaned phone numbers as per sql scripts

def clean_phone_number(phone_number, region="AE"):
    phone_number = str(phone_number) if pd.notna(phone_number) else ''
    if phone_number.strip() == '':
        return ''
    
    # Remove any spaces and non-digit characters except for '+'
    phone_number = ''.join(char for char in phone_number if char.isdigit() or char == '+')

    # Remove leading zeros and handle regional prefixes
    if phone_number.startswith('+'):
        number_without_plus = phone_number[1:]
    else:
        number_without_plus = phone_number

    number_without_plus = number_without_plus.lstrip('0')
    
    # Handle specific cases for phone number formatting
    if len(number_without_plus) == 9 and number_without_plus.startswith('5'):
        number_without_plus = '971' + number_without_plus
    elif number_without_plus.startswith('97105'):
        number_without_plus = '971' + number_without_plus[4:]  # Replace 97105 with 9715
    elif number_without_plus.startswith('96605'):
        number_without_plus = '966' + number_without_plus[4:]  # Replace 96605 with 9665

    elif number_without_plus.startswith('96505'):
        number_without_plus = '965' + number_without_plus[4:]

    elif number_without_plus.startswith('97405'):
        number_without_plus = '974' + number_without_plus[4:]

    elif number_without_plus.startswith('97305'):
        number_without_plus = '973' + number_without_plus[4:]
        
    cleaned_phone_number = '+' + number_without_plus
    
    # Validate the cleaned phone number
    try:
        parsed_number = phonenumbers.parse(cleaned_phone_number, region)
        is_valid = phonenumbers.is_valid_number(parsed_number)
        #print("Valid Number:", is_valid)
        return cleaned_phone_number if is_valid else ''
    except NumberParseException as e:
        #print(f"NumberParseException: {e}")
        return '' 
    

chunk['MOBILE_UNQ'] = chunk['Venue Parent Group Guest Phone Number'].apply(clean_phone_number)



######## Assigned country code

def assign_country_code_and_length(phone_number, default_region="US"):
    if pd.isna(phone_number) or not isinstance(phone_number, str):
        return pd.Series(["", None])
    
    try:
        if phone_number.startswith('+'):
            parsed_number = phonenumbers.parse(phone_number, None)
        else:
            parsed_number = phonenumbers.parse(phone_number, default_region)
        
        # Extract the country code
        country_code = str(parsed_number.country_code)
        country_code_length = len(country_code)
        
        # Calculate TOT_LENGTH excluding the country code
        tot_length = len(phone_number.replace('+', '').replace(country_code, '', 1))
        
        return pd.Series([region_code_for_number(parsed_number), tot_length])
    except phonenumbers.NumberParseException:
        return pd.Series(["", None])
 
chunk[['Country Code', 'TOT_LENGTH']] = chunk['MOBILE_UNQ'].apply(assign_country_code_and_length)



############# Email or phone Appears over 4 times ###########
email_counts = chunk['Valid Emails'].value_counts()
phone_counts = chunk['MOBILE_UNQ'].value_counts()
print("email_counts===>>",email_counts,"phone_counts==>>",phone_counts)

chunk.loc[chunk['Valid Emails'].isin(email_counts[email_counts > 4].index), 'Valid Emails'] = None
chunk.loc[chunk['MOBILE_UNQ'].isin(phone_counts[phone_counts > 4].index), 'MOBILE_UNQ'] = None

chunk['NB_COL'] = chunk.notna().sum(axis=1)




###### Duplicate MObile

#chunk['MOBILE_DUP3'] = chunk['Venue Parent Group Guest Phone Number'].where(chunk['MOBILE_UNQ'].duplicated(keep=False))

chunk['MOBILE_DUP3'] = chunk['MOBILE_UNQ'].where(chunk['MOBILE_UNQ'].duplicated(keep=False))



# --- Update Valid Emails in Mobile Duplicates ---


############ update most recent email and phone also get 2nd most recent under add_email and add_mobile block
chunk['Venue Parent Group Guest Profile Creation Date'] = pd.to_datetime(chunk['Venue Parent Group Guest Profile Creation Date'], errors='coerce')
chunk['NB_COL'] = pd.to_numeric(chunk['NB_COL'], errors='coerce')

# --- Update Valid Emails in Mobile Duplicates ---

# Sort by 'Venue Parent Group Guest Profile Creation Date' and 'NB_COL' to prioritize the most recent records
latest_emails = chunk.dropna(subset=['Valid Emails']) \
    .sort_values(by=['Venue Parent Group Guest Profile Creation Date', 'NB_COL'], ascending=[False, False])

# Get the most recent email
most_recent_emails = latest_emails.drop_duplicates(subset=['MOBILE_UNQ'], keep='first')

# Get the second most recent email
second_most_recent_emails = latest_emails[~latest_emails.index.isin(most_recent_emails.index)] \
    .drop_duplicates(subset=['MOBILE_UNQ'], keep='first')

# Merge with chunk to update Valid Emails and add additional_email
chunk = chunk.merge(most_recent_emails[['MOBILE_UNQ', 'Valid Emails']], 
                    on='MOBILE_UNQ', how='left', suffixes=('', '_new'))
chunk = chunk.merge(second_most_recent_emails[['MOBILE_UNQ', 'Valid Emails']], 
                    on='MOBILE_UNQ', how='left', suffixes=('', '_additional'))

# Update Valid Emails only if MOBILE_UNQ is not NaN
chunk['Valid Emails'] = chunk.apply(
    lambda row: row['Valid Emails_new'] if pd.notna(row['MOBILE_UNQ']) else row['Valid Emails'],
    axis=1
)

# Add additional_email only if it's different from the primary email and MOBILE_UNQ is not blank
chunk['additional_email'] = chunk.apply(
    lambda row: row['Valid Emails_additional'] if pd.notna(row['MOBILE_UNQ']) and row['Valid Emails_additional'] != row['Valid Emails'] else None,
    axis=1
)

# Drop the temporary columns
chunk = chunk.drop(columns=['Valid Emails_new', 'Valid Emails_additional'])

chunk['EMAIL_DUP3'] = chunk['Valid Emails'].where(chunk['Valid Emails'].duplicated(keep=False))

# --- Update Mobile in Email Duplicates ---

# Sort by 'Venue Parent Group Guest Profile Creation Date' and 'NB_COL' to prioritize the most recent records
latest_mobiles = chunk.dropna(subset=['MOBILE_UNQ']) \
    .sort_values(by=['Venue Parent Group Guest Profile Creation Date','NB_COL'], ascending=[False,False])

# Get the most recent mobile
most_recent_mobiles = latest_mobiles.drop_duplicates(subset=['Valid Emails'], keep='first')

# Get the second most recent mobile
second_most_recent_mobiles = latest_mobiles[~latest_mobiles.index.isin(most_recent_mobiles.index)] \
    .drop_duplicates(subset=['Valid Emails'], keep='first') 

# Merge with chunk to update MOBILE_UNQ and add additional_mobile
chunk = chunk.merge(most_recent_mobiles[['Valid Emails', 'MOBILE_UNQ']], 
                    on='Valid Emails', how='left', suffixes=('', '_new'))
chunk = chunk.merge(second_most_recent_mobiles[['Valid Emails', 'MOBILE_UNQ']], 
                    on='Valid Emails', how='left', suffixes=('', '_additional'))

# Update MOBILE_UNQ only if Valid Emails is not NaN
chunk['MOBILE_UNQ'] = chunk.apply(
    lambda row: row['MOBILE_UNQ_new'] if pd.notna(row['Valid Emails']) else row['MOBILE_UNQ'],
    axis=1
)

# Add additional_mobile only if it's different from the primary mobile number and Valid Emails is not blank
chunk['additional_mobile'] = chunk.apply(
    lambda row: row['MOBILE_UNQ_additional'] if pd.notna(row['Valid Emails']) and row['MOBILE_UNQ_additional'] != row['MOBILE_UNQ'] else None,
    axis=1
)

# Drop the temporary columns
chunk = chunk.drop(columns=['MOBILE_UNQ_new', 'MOBILE_UNQ_additional'])

################# endblock


################## create CUST_KEY 
chunk['CUST_KEY'] = chunk['MOBILE_UNQ'].fillna('') + chunk['Valid Emails'].fillna('')
print("@@@@@@@@@@",chunk['CUST_KEY'])
print("#####################",chunk['MOBILE_UNQ'].fillna('') + chunk['Valid Emails'].fillna(''))


########## add the additonal_email and mobile on the basis of  cust_key

latest_records = chunk.sort_values(by=['Venue Parent Group Guest Profile Creation Date', 'NB_COL'], ascending=[False, False])
most_recent_additional = latest_records.groupby('CUST_KEY').first().reset_index()
chunk = chunk.merge(most_recent_additional[['CUST_KEY', 'additional_email', 'additional_mobile']], 
                    on='CUST_KEY', 
                    how='left', 
                    suffixes=('', '_new'))

chunk['additional_email'] = chunk['additional_email_new'].combine_first(chunk['additional_email'])
chunk['additional_mobile'] = chunk['additional_mobile_new'].combine_first(chunk['additional_mobile'])

chunk = chunk.drop(columns=['additional_email_new', 'additional_mobile_new'])


###############
chunk.loc[chunk['additional_email'].isin(chunk['Valid Emails']), 'additional_email'] = ''

# Condition 2: Check additional_mobile against MOBILE_UNQ
chunk.loc[chunk['additional_mobile'].isin(chunk['MOBILE_UNQ']), 'additional_mobile'] = ''
###############




"""# Determine most recent cust_id for each CUST_KEY
latest_guest_ids = chunk.loc[chunk.groupby('CUST_KEY')['Venue Parent Group Guest Profile Creation Date'].idxmax()]
latest_guest_ids = latest_guest_ids[['CUST_KEY', 'Venue Parent Group Guest Guest ID']]
#latest_guest_ids.rename(columns={'Venue Parent Group Guest Guest ID': 'most_recent_guest_id'}, inplace=True)

chunk = chunk.merge(latest_guest_ids, on='CUST_KEY', how='left', suffixes=('', '_latest'))
# Assign the most recent guest ID to a new column
chunk['most_recent_guest_id'] = chunk['Venue Parent Group Guest Guest ID_latest']

#chunk['most_recent_guest_id'] = chunk.apply(lambda row: row['most_recent_guest_id'] if row['most_recent_guest_id'] != '9999999' else '9999999', axis=1)
chunk['most_recent_guest_id'] = chunk.apply(lambda row: '9999999' if pd.isna(row['Valid Emails']) and pd.isna(row['MOBILE_UNQ']) else '', axis=1)
#chunk.drop(columns=['cust_update_id_latest'], inplace=True)"""
  

# Determine most recent most_recent_guest_id for each CUST_KEY

latest_guest_ids = chunk.loc[chunk.groupby('CUST_KEY')['Venue Parent Group Guest Profile Creation Date'].idxmax()]
latest_guest_ids = latest_guest_ids[['CUST_KEY', 'Venue Parent Group Guest Guest ID']]
chunk = chunk.merge(latest_guest_ids, on='CUST_KEY', how='left', suffixes=('', '_latest'))
chunk['most_recent_guest_id'] = chunk['Venue Parent Group Guest Guest ID_latest']
chunk['most_recent_guest_id'] = chunk.apply(
    lambda row: '9999999' if pd.isna(row['Valid Emails']) and pd.isna(row['MOBILE_UNQ']) else row['most_recent_guest_id'],
    axis=1
)
chunk.drop(columns=['Venue Parent Group Guest Guest ID_latest'], inplace=True)



# Assigning ranks based on creation date
chunk['rank'] = (
    chunk.sort_values('Venue Parent Group Guest Profile Creation Date', ascending=False)
    .groupby('CUST_KEY').cumcount() + 1
).astype(str)

########## update gender  #####

chunk_sorted = chunk.sort_values(by=['most_recent_guest_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Gender'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))


chunk['Gender'] = chunk['Gender_most_recent']
chunk.drop(columns=['Gender_most_recent'], inplace=True)




########## Update Birthday #########

#chunk_sorted = chunk.sort_values(by=['most_recent_guest_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Birthday Date'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Birthday Date'] = chunk['Venue Parent Group Guest Birthday Date_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Birthday Date_most_recent'], inplace=True)



######### Update Company ########
#chunk_sorted = chunk.sort_values(by=['cust_update_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Guest Company'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Guest Company'] = chunk['Venue Parent Group Guest Guest Company_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Guest Company_most_recent'], inplace=True)


############### Update Job Title ####
#chunk_sorted = chunk.sort_values(by=['cust_update_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Guest Job Title'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Guest Job Title'] = chunk['Venue Parent Group Guest Guest Job Title_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Guest Job Title_most_recent'], inplace=True)



########## Update Salutation #####
chunk_sorted = chunk.sort_values(by=['most_recent_guest_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Salutation'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Salutation'] = chunk['Venue Parent Group Guest Salutation_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Salutation_most_recent'], inplace=True)


########## Update Status #########
chunk_sorted = chunk.sort_values(by=['most_recent_guest_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Guest Status'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Guest Status'] = chunk['Venue Parent Group Guest Guest Status_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Guest Status_most_recent'], inplace=True)



############### update full_name with the most recent non-null status for each cust_id

chunk_sorted = chunk.sort_values(by=['most_recent_guest_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Full Name'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Full Name'] = chunk['Venue Parent Group Guest Full Name_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Full Name_most_recent'], inplace=True)



######### -- UPDATE optin with the most recent non-null status for each cust_id

chunk_sorted = chunk.sort_values(by=['most_recent_guest_id', 'Venue Parent Group Guest Profile Creation Date'], ascending=[True, False])
most_recent_gender = chunk_sorted.groupby('most_recent_guest_id')['Venue Parent Group Guest Marketing Opt In - Parent Group (Yes / No)'].first().reset_index()
chunk = chunk.merge(most_recent_gender, on='most_recent_guest_id', how='left', suffixes=('', '_most_recent'))

chunk['Venue Parent Group Guest Marketing Opt In - Parent Group (Yes / No)'] = chunk['Venue Parent Group Guest Marketing Opt In - Parent Group (Yes / No)_most_recent']
chunk.drop(columns=['Venue Parent Group Guest Marketing Opt In - Parent Group (Yes / No)_most_recent'], inplace=True)



################## -------create first name and last name
chunk['first_name'] = chunk['Venue Parent Group Guest Full Name'].apply(lambda x: x.split(' ', 1)[0] if pd.notnull(x) else np.nan)
chunk['last_name'] = chunk['Venue Parent Group Guest Full Name'].apply(lambda x: x.split(' ', 1)[1] if pd.notnull(x) and ' ' in x else np.nan)



###


chunk.drop_duplicates(subset=["Valid Emails", "MOBILE_UNQ"], keep='first', inplace=True, ignore_index=True)
print("total mobile number is  is ===",len(chunk['MOBILE_UNQ']))
print("total email is ===",len(chunk['Valid Emails']))




columns_order = [
    'Venue Parent Group Guest Salutation',
    'Venue Parent Group Guest Full Name',
    'first_name',
    'last_name', 
    'Gender',
    'Venue Parent Group Guest Phone Number',
    'MOBILE_UNQ',
   'additional_mobile',
    #'MOBILE_TO',
    #'EMAIL_TO',
    'Country Code', 
    'TOT_LENGTH',
    'Venue Parent Group Guest Guest Email',
    
    'Valid Emails',
    'additional_email',
    'MOBILE_DUP3',
    'EMAIL_DUP3',
    'Invalid Emails',
    # 'Email_Status',
    #'cust_update_id_latest',
    'CUST_KEY',
    'most_recent_guest_id',
    'rank',
    'NB_COL',
    'Venue Parent Group Guest Birthday Date',
    'Venue Parent Group Guest Guest Company',
    'Venue Parent Group Guest Guest Job Title',
    'Venue Parent Group Guest Guest Status'
]


# Determine any additional columns that are not explicitly listed
remaining_columns = [col for col in chunk.columns if col not in columns_order]

# Combine the explicit order with any remaining columns
final_column_order = columns_order + remaining_columns

# Reorder the DataFrame columns
result = chunk[final_column_order]


# Save the result to CSV
result.to_csv(output_file, index=False)
print("result is generated....")

print("Phone numbers, country codes, and lengths processed and saved successfully.")


 