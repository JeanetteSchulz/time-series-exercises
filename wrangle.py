########################################### INFO ####################################################
# This is a Wrangle for obtaining data from python.zgulde.net only.                                 #
# Created By Jeanette Schulz                                                                        #


########################################## Imports ##################################################
import pandas as pd
import os
import requests



###################################### Data Requesting ##############################################

def grab_this(endpoint):
    """
    This function assumes you know an endpoint for python.zgulde.net and allows the user
    to get the specific data requested. If the data is on more than one page, the function
    loops through until it stops at the last page and returns all this data as single dataframe. 
    """
   
    # Requesting the data from the website
    response = requests.get(f'https://python.zgulde.net/api/v1/{endpoint}')
    
    # Storing data received from request
    data = response.json()
    
    # Creating initial dataframe
    df = pd.DataFrame(data['payload'][endpoint])
    
    # Creating page variable to be checked in while loop
    next_page = data['payload']['next_page']

    
    # Looping through remaining pages
    # First check to make sure there is a next page
    while data['payload']['next_page'] is not None:
        
        # Requesting information on next_page 
        response = requests.get('https://python.zgulde.net' + data['payload']['next_page'])
        data = response.json()

        # Assigning next next_page
        next_page = data['payload']['next_page']

        # Concatenating new page to dataframe
        df = pd.concat([df, pd.DataFrame(data['payload'][endpoint])]).reset_index(drop=True)

    return df

################################ Turning data into Dataframe ########################################

def fresh_superstore():
    """
    This function is time consuming, but will retreive a fresh dataframe from python.zgulde.net.
    This function requests all the data anew and then merges it into a single dataframe.
    It will also save all individual dataframes as csv files for later use.
    """
    # Informing the User
    print(f"Requesting Data...") 

    # Grabbing a fresh request for each dataframe
    items = grab_this('items')
    stores = grab_this('stores')
    sales = grab_this('sales')
    
    # Renaming some columns to make merging easier 
    sales = sales.rename(columns={'store': 'store_id', 'item': 'item_id'})
    
    # Informing the User
    print("Merging Data...") 

    # Merging sales with store
    df = pd.merge(sales, stores, on='store_id')

    # Merging sales with store
    df = pd.merge(df, items, on='item_id')
    
    # Informing the User
    print("Storing Data...") 

    # Create a csv of the final dataframe
    df.to_csv('superstore.csv')

    return df

######################################### Acquire ###################################################

def get_superstore():
    """
    This function utilizes pre-created csv files. It first checks for the superstore.csv
    and if this file has not been made yet, it will create it by calling all the csv
    files from the individual dataframes.
    """
    # Informing the User
    print("Checking if file exists...") 

    if os.path.isfile('superstore.csv'):
        # Informing the User
        print("Reading Data from file...") 
        
        # If csv file exists read in data from csv file.
        df = pd.read_csv('superstore.csv', index_col=0)
    
    else:
         # Informing the User
        print("Creating New file...") 
        
        df = fresh_superstore()

    # Informing the User
    print("Done!") 
                    
    return df