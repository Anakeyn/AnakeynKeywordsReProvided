# -*- coding: utf-8 -*-
"""
Created on Sat Jun 19 18:50:19 2021

@author: Pierre
"""
################################################################################
#Anakeyn Keywords Re Provided v0.1 
#This tool import data keywords information from Google Search Console in 
#order to fill Keywords information in Google Analytics Data to avoid "Not Provided"
#keywords.
###############################################################################

#from  myconfig import MYCLIENTID, MYCLIENTSECRET, ACCOUNT_ID, WEBPROPERTY_ID, VIEW_ID, SITE_URL
from  config import MYCLIENTID, MYCLIENTSECRET, ACCOUNT_ID, WEBPROPERTY_ID, VIEW_ID, SITE_URL


#import needed libraries

import pandas as pd  #for Dataframes 
#import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta
from time import sleep
import os

print(os.getcwd())  #check

#############################################################################
# Before you need to create a project in Google Developpers :
# https://console.developers.google.com/
# Choose APIs :
# - Google Analytics Reporting API  #V4 
# - Google Search Console API #V3
# 

#Google Code

import argparse #Parser for command-line options, arguments and sub-commands

#Connection
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
          'https://www.googleapis.com/auth/analytics.edit', 'https://www.googleapis.com/auth/webmasters']

# 'https://www.googleapis.com/auth/analytics', not necessary
# Parse command-line arguments.
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])
flags = parser.parse_args([])


# Create a flow object for oAuth MYCLIENTID ou MYCLIENTSECRET connection
flow = client.OAuth2WebServerFlow(client_id=MYCLIENTID,
                           client_secret=MYCLIENTSECRET,
                           scope=SCOPES)

# Prepare credentials, and authorize HTTP object with them.
# If the credentials don't exist or are invalid run through the native client
# flow. The Storage object will ensure that if successful the good
# credentials will get written back to a file.
storage = file.Storage('akrpcredentials.dat')
credentials = storage.get()

######################################################
#Here a windows will open in .dat file doesn't exist
if credentials is None or credentials.invalid:
  credentials = tools.run_flow(flow, storage, flags)
http = credentials.authorize(http=httplib2.Http())






#######################################################################    
#Transform Google Analytics response in dataframe
#see here : 
#https://www.themarketingtechnologist.co/getting-started-with-the-google-analytics-reporting-api-in-python/
          
def dataframe_response(response):
  list = []
  # get report data
  for report in response.get('reports', []):
    # set column headers
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])
    
    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
          dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
          for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
            if ',' in value or '.' in value:
              dict[metric.get('name')] = float(value)
            else:
              dict[metric.get('name')] = int(value)

        list.append(dict)
    
    df = pd.DataFrame(list)
    return df
######################################################################


#########################################################################
# get DATA from Analytics V4 (Google Analytics Reporting API )
#https://developers.google.com/analytics/devguides/reporting/core/v4
##########################################################################
#Max 9 dimensions , 10 metrics
#https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/

#create severall start dates
OneDay = date.today()
Last7Days = date.today() - timedelta(days=7)
Last28Days = date.today() - timedelta(days=28)
Last3Months = date.today() - timedelta(days=30*3)
Last6Months = date.today() - timedelta(days=30*6)
Last12Months = date.today() - timedelta(days=30*12)
Last15Months = date.today() - timedelta(days=30*15) 
Last16Months = date.today() - timedelta(days=30*16)


#Choose one start date
myStartDate = Last16Months
if myStartDate == Last16Months :
    myGSCStartDate = myStartDate
else :
    myGSCStartDate = myStartDate - timedelta(days=30) #for Google Search Console add 30 Days to get more data

#in strings :    
myStrStartDate = myStartDate.strftime("%Y-%m-%d")
myStrGSCStartDate = myGSCStartDate.strftime("%Y-%m-%d")
myStrEndDate = date.today().strftime("%Y-%m-%d")


def get_dfGA(analyticsV4, VIEW_ID, ACCOUNT_ID):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analyticsV4.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': 100000,  #to exceed the limit of 1000
          'dateRanges': [{'startDate': myStrStartDate, 'endDate': myStrEndDate}],
          'metrics': [{'expression': 'ga:users'},{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:date'},
                         {'name': 'ga:sourceMedium'},  
                         {'name': 'ga:countryIsoCode'},  #Country ISO-3166-1 alpha-2
                         {'name': 'ga:deviceCategory'}, 
                         {'name': 'ga:keyword'},
                         {'name': 'ga:landingPagePath'}]
        }]
      },
      quotaUser = ACCOUNT_ID
  ).execute()

    
# Build the service object. V4 Google Analytics API
analyticsV4 = build('analyticsreporting', 'v4', credentials=credentials)


response = get_dfGA(analyticsV4, VIEW_ID, ACCOUNT_ID)  #get data from Google Analytics

#Data From  GA in DataFrame
dfGA = dataframe_response(response) #transform response in dataframe


#Change columns names in order to avoid "ga:" in name
dfGA = dfGA.rename(columns={'ga:date': 'date', 
                            'ga:sourceMedium': 'sourceMedium',
                            'ga:countryIsoCode': 'countryIsoCode2',
                            'ga:deviceCategory': 'deviceCategory',
                            'ga:keyword': 'keyword',
                            'ga:landingPagePath': 'landingPagePath',
                            'ga:users': 'users',
                            'ga:sessions': 'sessions'})

#Entire page URL
dfGA['page']=dfGA.apply(lambda x : SITE_URL + x['landingPagePath'],axis=1)

#transforma date string in datetime 
dfGA.date = pd.to_datetime(dfGA.date,  format="%Y%m%d")

#Tidy dfGA according to sessions
dfGA = dfGA.loc[dfGA.index.repeat(dfGA['sessions'])] #split in multiple rows according to sessions value
dfGA.reset_index(inplace=True, drop=True)  #reset index
dfGA['sessions'] = 1 #all sessions to one.
dfGA =  dfGA.drop(columns=['users'])  #remove users column (not used)

#verifs
#dfGA.dtypes
#dfGA.count() 
#dfGA.head(20)

#Save in excel
#dfGA.to_excel("dfGA.xlsx", sheet_name='dfGA', index=False)  


#Select only google / organic
dfGAGoogleOrganic = dfGA.loc[dfGA['sourceMedium'] == 'google / organic']
dfGAGoogleOrganic.reset_index(inplace=True, drop=True)  #reset index
#Save in Excel
#dfGAGoogleOrganic.to_excel("dfGAGoogleOrganic.xlsx", sheet_name='dfGAGoogleOrganic', index=False)  

###################################################################################
#########################################################################
# get DATA from Google Search Console
##########################################################################
#open a Google Search console service (previously called Google Webmasters tools)
webmastersV3 = build('webmasters', 'v3', credentials=credentials)




dfGSC = pd.DataFrame()  #global dataframe for precise traffic

       
#################################################################################################
####### Get Precise traffic from Google Search Console :   clicks, impressions, ctr, poition, date, country, device 
#  +  Queries and landing Page ##############
# Beware : When we ask for "precise traffic" i.e : with query and landing page we don't get all traffic !

dfGSC = pd.DataFrame()  #global dataframe for precise traffic
        
maxStartRow = 1000000000 #to avoid infinite loop
myStartRow = 0
        
while ( myStartRow < maxStartRow):
    df = pd.DataFrame() #dataframe for this loop
   
    mySiteUrl = SITE_URL
    myRequest = {
        'startDate': myStrGSCStartDate,    #older date for Google Search Console (1 month more than GA)
        'endDate': myStrEndDate,      #most recent date
        'dimensions':  ["date", "query","page","country","device"],      #Country ISO 3166-1 alpha-3  
        'searchType': 'web',         #for the moment only Web 
        'rowLimit': 25000,         #max 25000 for one Request 
        'startRow' :  myStartRow                #  for multiple resquests 'startRow':
        }

    response =  webmastersV3.searchanalytics().query(siteUrl=mySiteUrl, body=myRequest, quotaUser=ACCOUNT_ID).execute()

    print("myStartRow:",myStartRow)
    #set response (dict) in DataFrame for treatments purpose.
    df = pd.DataFrame.from_dict(response['rows'], orient='columns')

    if ( myStartRow == 0) :
        dfGSC = df  #save the first loop df in global df
    else :
        dfGSC = pd.concat([dfGSC, df], ignore_index=True) #concat  this loop df  with  global df

    if (df.shape[0]==25000) :
        myStartRow += 25000  #continue
    else :
        myStartRow = maxStartRow+1  #stop
        
#split keys in date query page country device
dfGSC[["date", "query", "page", "country", "device"]] = pd.DataFrame(dfGSC["keys"].values.tolist())
dfGSC.date = pd.to_datetime(dfGSC.date,  format="%Y-%m-%d")
dfGSC =  dfGSC.drop(columns=['keys'])  #remove Keys (not used)
       

#Save in Excel
#dfGSC.to_excel("dfGSC.xlsx", sheet_name='dfGSC', index=False)  

###########################################################################################
# Tidy the Dataframe (one row by observation, one column by variable)
###########################################################################################
#Create rows with impressions  whithout clicks
dfGSC['ImpressionsMinusClicks']=dfGSC.apply((lambda x : x['impressions'] - x['clicks']),axis=1)
dfGSCImpressions = dfGSC.loc[dfGSC.index.repeat(dfGSC['ImpressionsMinusClicks'])] #split in multiple rows according to 'ImpressionsMinusClicks' value
dfGSCImpressions.reset_index(inplace=True, drop=True)  #reset index
dfGSCImpressions['impressions'] = 1 #all impressions to one.
dfGSCImpressions['clicks'] = 0  #all clicks to zero.
#Create rows with clicks
dfGSCClicks = dfGSC.loc[dfGSC.index.repeat(dfGSC['clicks'])] #split in multiple rows according to clicks value
dfGSCClicks.reset_index(inplace=True, drop=True)  #reset index
dfGSCClicks['impressions'] = 1  #all impressions to one.
dfGSCClicks['clicks'] = 1  #all clicks to one.


dfGSCTidy = pd.concat([dfGSCImpressions, dfGSCClicks ])
dfGSCTidy.reset_index(inplace=True, drop=True)  #reset index
#sort 
dfGSCTidy.sort_values(by=["date", "clicks", "country", "device", ], ascending=[True, False, True, True],inplace=True)
dfGSCTidy.reset_index(inplace=True, drop=True)  #reset index


#remove 'ImpressionsMinusClicks' column, not needed anymore
dfGSCTidy.drop(columns='ImpressionsMinusClicks', inplace=True)


###############################################################################################
#calculate a "weight" for the futur weight sample according to "actual" ctr and theorical Ctr according to position
#source https://www.smartinsights.com/search-engine-optimisation-seo/seo-analytics/comparison-of-google-clickthrough-rates-by-position/


def calculateWeight(ctr, position) :
    ctrFirstPage = [0.342, 0.171, 0.114, 0.081, 0.074, 0.051, 0.041, 0.033, 0.029, 0.026]
    ctrSecondPage = 0.02
    ctrOtherPage = 0.01
    if ctr>0.0 :
        weight = ctr
    elif position < 11.0 :
        weight = ctrFirstPage[int(position)-1]
    elif ((position>10.0) & (position<21.0)) :
        weight = ctrSecondPage
    else :
        weight = ctrOtherPage
    return weight



dfGSCTidy['weight']= dfGSCTidy.apply(lambda x: calculateWeight(x.ctr, x.position), axis=1)


#Save in Excel
#dfGSCTidy.to_excel("dfGSCTidy.xlsx", sheet_name='dfGSCTidy', index=False)   



########################################################################
# Get Country Isocode 2  to compare with GA traffic
# Read Country Code data

dfCountryCodes = pd.read_csv("countries_codes_and_coordinates.csv", sep=';', usecols=["countryIsoCode3","countryIsoCode2"])
dfCountryCodes['countryIsoCode3']=dfCountryCodes['countryIsoCode3'].str.strip() #need to do that to avoid NaN in Merge
dfCountryCodes['countryIsoCode2']=dfCountryCodes['countryIsoCode2'].str.strip() #need to do that to avoid NaN in Merge

dfCountryCodes.dtypes
dfCountryCodes.info()
#Merge dfGSCTidy with  Country Codes

dfGSCTidy['countryIsoCode3'] = dfGSCTidy['country'].apply(lambda x: x.upper())
dfGSCTidy['countryIsoCode3'] = dfGSCTidy['countryIsoCode3'].str.strip() #need to do that to avoid NaN in Merge
dfGSCTidy.info()

dfGSCTidyCountries= pd.merge(left=dfGSCTidy, right=dfCountryCodes, how='left', left_on='countryIsoCode3', right_on='countryIsoCode3')

#Save in Excel
#dfGSCTidyCountries.to_excel("dfGSCTidyCountries.xlsx", sheet_name='dfGSCTidyCountries', index=False) 

dfGSCTidyCountries.dtypes
dfGSCTidyCountries.date = pd.to_datetime(dfGSCTidyCountries.date,  format="%Y-%m-%d")

#########################################################################################
########################################################################################
###############################Change (not Provided)  with good query in dfGAGoogleOrganic
dfGSCTidyCountries.dtypes
dfGSCTidyCountries['device'].unique()

#Prepare dfGAReProvided
dfGAReProvided = dfGAGoogleOrganic
dfGAReProvided.dtypes
dfGAReProvided['sourceMedium']
dfGAReProvided['countryIsoCode2']
dfGAReProvided['deviceCategory'] = dfGAReProvided['deviceCategory'].str.upper()
dfGAReProvided['page'] = SITE_URL + dfGAReProvided['landingPagePath']
dfGAReProvided['clicks'] = ""
dfGAReProvided['impressions'] = ""
dfGAReProvided['ctr'] = ""
dfGAReProvided['position'] = ""
dfGAReProvided['GSCDate'] = ""
dfGAReProvided['weight'] = ""
dfGAReProvided['factors'] = ""

for index, row in dfGAReProvided.iterrows():
    print("index:", index)
    print("date:", row['date'])
    print("page:", row['page'])
    print("countryIsoCode2:", row['countryIsoCode2'])
    print("deviceCategory:", row['deviceCategory'])
    #select page date country device rows in dfGSCTidy according 
    factors = 4
    print("4 factors")
    dfSelection = dfGSCTidyCountries.loc[(dfGSCTidyCountries['page']==row['page']) & (dfGSCTidyCountries['date']==row['date']) & (dfGSCTidyCountries['countryIsoCode2']==row['countryIsoCode2']) & (dfGSCTidyCountries['device']==row['deviceCategory'])]
    if dfSelection.size==0 :
        factors=3
        print("3 factors")
        dfSelection = dfGSCTidyCountries.loc[(dfGSCTidyCountries['page']==row['page']) & (dfGSCTidyCountries['countryIsoCode2']==row['countryIsoCode2']) & (dfGSCTidyCountries['device']==row['deviceCategory']) ]
    if dfSelection.size==0 :
        factors=2
        print("2 factors")
        dfSelection = dfGSCTidyCountries.loc[(dfGSCTidyCountries['page']==row['page'])  & (dfGSCTidyCountries['device']==row['deviceCategory'])]
    if dfSelection.size==0 :
        factors=1
        print("1 factor")
        dfSelection = dfGSCTidyCountries.loc[(dfGSCTidyCountries['page']==row['page'])]
    if dfSelection.size==0 :
        factors=0
        print("None factor")
        #dfSelection = dfGSCTidyCountries
    #sleep(1)
    
    
    if factors > 0 : 
        #Sample data
        dfSampleRow = dfSelection.sample(n=1, weights=dfSelection['weight'])
        dfSampleRow.reset_index(inplace=True, drop=True)  #reset index
        #Remove randomly selected row in dfGSCTidy for next loop
        #dfGSCTidy = dfGSCTidy.drop(dfSampleRow['myIndex'])
    
        mySampleRow = dfSampleRow.iloc[0,:] #df to series
    
        ##############  insert Data
   
        dfGAReProvided.loc[index,'keyword']=mySampleRow['query']
        dfGAReProvided.loc[index,'clicks'] = mySampleRow['clicks']
        dfGAReProvided.loc[index,'impressions'] = mySampleRow['impressions']
        dfGAReProvided.loc[index,'ctr'] = mySampleRow['ctr']
        dfGAReProvided.loc[index,'position'] = mySampleRow['position']
        dfGAReProvided.loc[index,'GSCDate'] = mySampleRow['date']
        dfGAReProvided.loc[index,'weight'] = mySampleRow['weight']
        dfGAReProvided.loc[index,'factors'] = factors
        
    else :
        #No data Found
        dfGAReProvided.loc[index, 'keyword'] = "(Not Found)"
        dfGAReProvided.loc[index,'factors'] = factors


#Save in Excel
dfGAReProvided.to_excel("dfGAReProvided.xlsx", sheet_name='dfGAReProvided', index=False) 


############ END END END

