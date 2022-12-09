#@Hans Zamora Carrillo -> Fulcrum Data Engineer Test
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd    
import pandasql as ps


#start web page URL 
INI_URL = "http://54.211.14.56/"

#get URL session
def get_url_session(lsUrl):
    try:
        response = requests.get(lsUrl)
    except requests.exceptions.RequestException as e:
        print(str(e))
        raise e
    else:
        return response

#renaming columns from json
def rename_json(jsInput, dictMapColumns):

    try:
        for oldColumn, newColumn in dictMapColumns.items(): 
            jsInput[newColumn] = jsInput.pop(oldColumn)  
        return jsInput
    except Exception as e:
        print(str(e))
        raise e

#getting html content from URL
def get_html_content(urlSession):    
    try:
        html = BeautifulSoup(urlSession.text, 'html.parser')
    except Exception as e:
        print(str(e))
        raise e
    else:
        return html

#gettin cookies from URL
def get_web_page_cookies(urlSession):
    try:
        cookies = urlSession.cookies.get_dict()
    except Exception as e:
        print(str(e))
        raise e
    else:
        return cookies

#getting secret msg
def get_secret_msg(secret, scrambDic):
    secret_msg = ""

    try: 
        #invert scramble dict [key :value] to -> [value : key]
        reverse_scrm_dict = {
                                value: key for key, value in scrambDic.items()
                            }    

        #getting original positions for each character
        reverse_scrm_dict = dict(sorted(reverse_scrm_dict.items()))                        

        #unScrambling message
        for valor in reverse_scrm_dict.values():
            secret_msg = secret_msg + secret[int(valor)] 

    except Exception as e:
        print(str(e))
        raise e
    else:
        return secret_msg

#writting a Json File in the local directory
def write_json_file(df, fileName):
    try:
        df.to_json(fileName, orient = 'records', compression = 'infer', index = 'true')
        print(f"File: {fileName} -> created successfully")
    except Exception as e:
        print(str(e))
        raise e    

#writting a Json File in the local directory
def export_secret_file(secret, fileName):
    try:
        text_file = open(fileName, "wt")
        text_file.write(secret)
        text_file.close()
    except Exception as e:
        print(str(e))
        raise e     
    else:
        print(f"File: {fileName} created succesfully\n")      

def main():

    #index from webpage
    startIndex = 1

    #list to storage all the HTML content from web page
    htmlList = []

    #storate the cookies values from "super_secret_secret" cookie
    superSecret = ""

    #searching all htlm pages with valid structure
    while startIndex > 0:

        startUrl = f"{INI_URL}{startIndex}"

        sessionfulcrum = get_url_session(startUrl)
        fulcrumPageHtml = get_html_content(sessionfulcrum)       
        
        cookie = get_web_page_cookies(sessionfulcrum)

        #concat all cookies values
        if 'super_secret_secret' in cookie:
            superSecret += str(cookie['super_secret_secret'])

        if fulcrumPageHtml.find('script') is not None:
            startIndex = startIndex + 1
            startUrl = f"{INI_URL}{startIndex}"        
            htmlList.append(fulcrumPageHtml)                    
        else:
            break

    #dataframe to storage the raw data from payloads
    df_allListings = pd.DataFrame()

    for html in htmlList:

        #getting scramble-mapping dict
        scrableMapping = html.find('script', {'id':'scramble-mapping'})

        if scrableMapping is not None:
           dictScrambling = json.loads(scrableMapping.text.strip()[:-1].split("=")[1])
        
        #getting payload info
        payload = html.find('script', {'id':'payload'}) 

        #split by "=" for getting just the json part
        jsonPayload = json.loads(payload.text.strip()[:-1].split("=")[1])

        #dict for mapping columns names from old payload version to the new one
        dictRenameOldFormatToNew = {'baths' : 'bathrooms', 'bedrooms' : 'rooms', 'lat' : 'latitude'
                                    ,'listing_id' : 'uid' , 'listing_title' : 'name'
                                    ,'lng' : 'longitude', 'location' : 'city', 'nightly_rate' : 'price'
                                    ,'nightly_rate_currency' : 'currency' ,'provider' : 'provider_id'
                                    ,'star_rating' : 'rating', 'user_profile': 'user'}

        #validating payload format
        if jsonPayload['payload_format'] == 'new':        
            jsData = jsonPayload['data']['details']     

        #when the payload format is "old" I re-mapping all the columns using dict: ↑↑ "dictRenameOldFormatToNew"
        else: 
            jsData = jsonPayload['listing_details']
            jsData = rename_json(jsData, dictRenameOldFormatToNew)        

        #extracting user information
        jsUser = jsData['user']

        #deleting user key from payload
        del jsData['user']

        #renaming 'user_name' key from json "user" 
        jsUser['user_name'] = jsUser.pop('name')

        #joining user information with listing info for being use as RAW data
        jsData.update(jsUser)

        #creating Dataframe for listings
        df_listings = pd.json_normalize(jsData)
        df_allListings = pd.concat([df_allListings, df_listings],ignore_index=True)

    #********************** Start Data Normalization  and exporting of .json Files ************************"#

    #Note: I use pandasql for transforming the raw data, for sure there are other ways to do this, but for now it works

    #User Table
    #getting the unique values for USER
    lsSql = """
        select distinct user_name
            ,is_super_host as super_host
            ,picture_url
        from df_allListings d
    """
    #creating dataframe using pandasql
    tbl_user = ps.sqldf(lsSql)

    #restarting dataframe index to be use as a Unique ID (Primary Key) for table User
    tbl_user.index = tbl_user.index + 1
    tbl_user = tbl_user.rename_axis('user_id').reset_index()

    print("tbl_user --->>>")    
    print(tbl_user)       
    print("\n") 


    #Note, I figured out that there user_id: 801 is duplicated in the raw data, 
    #so for that reason I decided to recreate the user_id for getting a unique value

    #writing tbl_user.json file
    write_json_file(tbl_user, 'tbl_user.json')   

    #tbl_currency

    lsSql = """
        select distinct currency as currency_code
        from df_allListings d
    """
    tbl_currency = ps.sqldf(lsSql)   

    #restarting dataframe index to be use as a Unique ID (Primary Key) for table currency
    tbl_currency.index = tbl_currency.index + 1
    tbl_currency = tbl_currency.rename_axis('currency_id').reset_index()

    print("tbl_currency --->>>")    
    print(tbl_currency)    
    print("\n")     

    #writing tbl_currency.json file
    write_json_file(tbl_currency, 'tbl_currency.json')    

    #tbl_location

    lsSql = """
        select distinct city as location_name
            ,latitude
            ,longitude
        from df_allListings d
    """
    tbl_location = ps.sqldf(lsSql)
    
    #restarting dataframe index to be use as a Unique ID (Primary Key) for table currency
    tbl_location.index = tbl_location.index + 1
    tbl_location = tbl_location.rename_axis('location_id').reset_index()
    
    print("tbl_location --->>>")    
    print(tbl_location)
    print("\n")     

    #writing tbl_location.json file
    write_json_file(tbl_location, 'tbl_location.json')

    #tbl_listing

    lsSql = """
        select d.uid as listing_id
            ,c.currency_id
            ,l.location_id
            ,u.user_id
            ,d.provider_id as provider
            ,d.name as listing_title
            ,d.rooms
            ,d.bathrooms
            ,d.price
            ,d.rating
        from df_allListings d
            join tbl_currency c on c.currency_code = d.currency
            join tbl_location l on l.location_name = d.city --and l.latitude = d.latitude and l.longitude = d.longitude --> optional
            join tbl_user u on u.user_name = d.user_name    --> We can add other fields but with the current data isn't necessary
    """
    tbl_listing = ps.sqldf(lsSql)

    print("tbl_listing --->>>")    
    print(tbl_listing)
    print("\n")     

    #writing tbl_listing.json file
    write_json_file(tbl_listing, 'tbl_listing.json')

    #********************** End Data Normalization  and exporting of .json Files ************************"#    

    print("hidden message --->>>")
    print(get_secret_msg(superSecret, dictScrambling))
    print("\n") 

    export_secret_file(get_secret_msg(superSecret, dictScrambling),'secret.txt')


if __name__ == "__main__":
    main()    




