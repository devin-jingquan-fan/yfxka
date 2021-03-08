# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 11:17:29 2020

@author: ptmind
"""

import requests
from shareplum import Office365
from config import config

# get data from configuration
username = config['sp_user']
password = config['sp_password']
site_name = config['sp_site_name']
base_path = config['sp_base_path']
doc_library = config['sp_doc_library']

file_name = "cat_pic.jpg"

# Obtain auth cookie
authcookie = Office365(base_path, username=username, password=password).GetCookies()
session = requests.Session()
session.cookies = authcookie
session.headers.update({'user-agent': 'python_bite/v1'})
session.headers.update({'accept': 'application/json;odata=verbose'})


# perform the actual upload
with open( file_name, 'rb') as file_input:
    try: 
        response = session.post( 
            url=base_path + "/sites/" + site_name + "/Shared%20Documents/Forms/AllItems.aspx/_api/web/GetFolderByServerRelativeUrl('" + doc_library + "')/Files/add(url='" 
            + file_name + "',overwrite=true)",
            data=file_input)
    except Exception as err: 
        print("Some error occurred: " + str(err))
        
        
config.py

config = dict()
config['sp_user'] = 'email'
config['sp_password'] = 'pass
config['sp_base_path'] = 'https://bboxxeng.sharepoint.com'
config['sp_site_name'] = 'TESTIAN'
config['sp_doc_library'] = 'Test'


response = session.post( 
            url=base_path + "/sites/" + site_name + "/_api/web/GetFolderByServerRelativeUrl('Shared%20Documents/"+doc_library+"')/Files/add(url='" 
            + file_name + "',overwrite=true)",
            data=file_input)