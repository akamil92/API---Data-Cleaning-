from flask import Flask, request
import pandas as pd
import numpy as np

app = Flask(__name__)

    
def Datacleaning(company_name,rod_date,stock_table):

    date_str = rod_date
    Name_of_company = company_name

 

    # Get the date value from the user and convert it to a pandas datetime object

    date_value = pd.to_datetime(date_str)

    # Add the date column to the dataframe
    stock_table.insert(loc=0, column='Date', value=date_value)

    # Convert the date column to the desired format
    stock_table['Date'] = stock_table['Date'].dt.strftime('%Y-%m-%d')


    # Add the 'Company' column to the dataframe
    stock_table.insert(loc=0, column='Company', value = Name_of_company)


    # Delete the rows that do not have a value in the 'Shareholding' column
    stock_table.dropna(subset=['Investor_Type'], inplace=True)


    # Create a new DataFrame containing the dropped rows
    dropped_stock_table = stock_table[~stock_table['Investor_Type'].notna()]


    #Started data cleaning
    #Rearrange the Type of shareholders into INDIVIDUAL, CORPORATE AND INSTITUTIONAL
    ## Add a new column to the DataFrame

    stock_table['Shareholder TYPES(new sort)'] = ''

    # Iterate over the rows in the DataFrame and update the new column based on the Investor_Type column
    for index, row in stock_table.iterrows():
        investor_type = row['Investor_Type']
        if investor_type in ["RETAIL BODY CORPORATES - PRIVATE LIMITED COMPANY", "CORPORATE BODY - BANK/FINANCE COMPANY", "RETAIL BODY CORPORATES - PRIVATE LIMITED COMPANY", "CORPORATE BODY - CO-OPERATIVES","RETAIL BODY CORPORATES - LIMITED COMPANY"]:
            stock_table.at[index, 'Shareholder TYPES(new sort)'] = 'CORPORATE'
        

        elif investor_type in ["INSTITUTIONAL CORPORATES - LIMITED COMPANY","INSTITUTIONAL BODY CORPORATES - LIMITED COMPANY","CORPORATE BODY - INVESTMENT TRUST/FOUNDATION", "OTHERS"]:
            stock_table.at[index, 'Shareholder TYPES(new sort)'] = 'INSTITUTIONAL'
            
        elif investor_type in ["INDIVIDUAL", "CORPORATE BODY - NOMINEE COMPANY"]:
            stock_table.at[index, 'Shareholder TYPES(new sort)'] = 'INDIVIDUAL'    
            
        else : 
            raise Exception(f"{index},{stock_table.at[index,'Member_No']},{investor_type} Unexpected investor type")

            
            #check the individual
            #error alert
            


    # Add new columns which is 'Pledge(Broker)' and 'Pledge(Shareholder)' to the DataFrame 

    stock_table['Pledge(Broker)'] = ''
    stock_table['Pledge(Shareholder)'] = ''

    # Iterate over the rows in the DataFrame and update the new columns based on the Qualifier column
    for index, row in stock_table.iterrows():
        qualifier = row['Qualifier']
    
        pledge_parts = str(qualifier).split(' FOR ')
        if len(pledge_parts) == 2:
            stock_table.at[index, 'Pledge(Broker)'] = pledge_parts[0].strip()
            stock_table.at[index, 'Pledge(Shareholder)'] = pledge_parts[1].strip()
        else:
            stock_table.at[index, 'Pledge(Broker)'] = pledge_parts[0].strip()



    #To delete "nan" value in the column 'Pledge(Broker)'

    stock_table['Pledge(Broker)'] = stock_table['Pledge(Broker)'].fillna('')


    stock_table.dropna(subset=['Pledge(Broker)'], inplace=True)


    #Copy PLedge(Shareholder) column into the Combine Name(CDS+NOMINEE) column
    stock_table['Combine Name (CDS + NOMINEE)'] = stock_table['Pledge(Shareholder)']


    # Copy data from Qualifier column to 'Combine Name (CDS + NOMINEE) column for empty cells only
    stock_table['Combine Name (CDS + NOMINEE)'] = stock_table['Combine Name (CDS + NOMINEE)'].replace('', pd.NA).fillna(stock_table['Qualifier'])



    # Copy data from Holder_Name column to 'Combine Name (CDS + NOMINEE) column for empty cells only
    stock_table['Combine Name (CDS + NOMINEE)'] = stock_table['Combine Name (CDS + NOMINEE)'].replace('', pd.NA).fillna(stock_table['Holder_Name'])



    # Delete the brackets
    #PLEDGED SECURITIES ACCOUNT FOR SIA SIEW SIN (SIA0040C) --> deleting brackets onwords

    stock_table['Pledge(Shareholder)'] = stock_table['Pledge(Shareholder)'].str.replace(r'\(.*', '', regex=True)


    #Create gender colunn
    #to add more characteristic --> ic length & whole thing number with format ic 

    import re 
    for index, row in stock_table.iterrows():
        investor_type = row['Investor_Type']
        investor_id = row['Investor_ID']
        if investor_type == 'INDIVIDUAL' and len(investor_id) == 14 and re.match(r'^\d{6}-\d{2}-\d{4}$', investor_id):
            last_digit = int(investor_id[-1])
            if last_digit % 2 == 0:
                stock_table.at[index, 'Gender'] = 'FEMALE'
            else:
                stock_table.at[index, 'Gender'] = 'MALE'



    # Iterate over the rows in the DataFrame and update the new column based on the Investor_ID column
    for index, row in stock_table.iterrows():
        investor_type = row['Investor_Type']
        investor_id = row['Investor_ID']
        if investor_type == 'INDIVIDUAL':
            if investor_id[:2].isdigit():
                first_two_digits = int(investor_id[:2])
                if 20 <= first_two_digits <= 99:
                    stock_table.at[index, 'Year_of_Birth'] = 1900 + first_two_digits
                elif 0 <= first_two_digits <= 20:
                    stock_table.at[index, 'Year_of_Birth'] = 2000 + first_two_digits
                else:
                    continue
            else:
                continue



    stock_table['Combine Name (CDS + NOMINEE)'] = stock_table['Combine Name (CDS + NOMINEE)'].str.replace(r'\(.*\)', '', regex=True)




    # Replace IVT with data from Holder_Name and Pledge(Broker) in Combine Name (CDS + NOMINEE) column
    for index, row in stock_table.iterrows():
        if 'IVT' in row['Combine Name (CDS + NOMINEE)']:
            holder_name = row['Holder_Name']
            pledge_broker = row['Pledge(Broker)']
            new_name = holder_name + ' (' + pledge_broker + ')'
            stock_table.at[index, 'Combine Name (CDS + NOMINEE)'] = new_name


    #TO delete any whitespace in column 'Combine Name (CDS + NOMINEE)'


    stock_table['Combine Name (CDS + NOMINEE)'] = stock_table['Combine Name (CDS + NOMINEE)'].str.strip()


    #Rearrange the squence of Column 

    stock_table = stock_table.reindex(columns=['Company','Investor_ID', 'Date', 'Member_No', 'Gender', 'Year_of_Birth', 'P_Code', 'P_Branch', 'P_Name', 'CDS', 'Holder_Name', 'Address_1', 'Address_2', 'Town', 'Postcode', 'State', 'Country', 'Nation', 'Race', 'Stock_Code', 'Shareholding', 'Beneficiary', 'Qualifier', 'Investor_Type', 'Shareholder TYPES(new sort)', 'Pledge(Broker)', 'Pledge(Shareholder)', 'Combine Name (CDS + NOMINEE)'])

    # End of Data Cleaning 
    
    #Save the updated DataFrame as a CSV file
    stock_table.to_csv('cleaned_data_clean_data_2023-03-02.csv', index=False)


    return stock_table

@app.route('/api/upload_csv', methods=['POST'])
def upload_data():
    # Get the CSV file from the request
    csv_file = request.files.get('csv_file')
    if not csv_file:
        return 'No CSV file received', 400
    if csv_file.mimetype != 'text/csv':
        return 'Invalid file type. Please upload a CSV file.', 400

    # Get the company_name and rod_date from the request
    company_name = request.form.get('company_name')
    rod_date = request.form.get('rod_date')
    if not company_name or not rod_date:
        return 'Company name and ROD date are required', 400
    
    stock_table = pd.read_csv(csv_file)


    # Process the CSV file and additional data here

    cleaned_data = Datacleaning(company_name,rod_date,stock_table)


    text = company_name + '-' + rod_date
    return 'Data received ' + text


if __name__ == '__main__':
        app.run()
