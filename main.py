import pandas as pd
import numpy as np
import re
from urllib.parse import urlparse
import boto3
import sys
import os
import pandas as pd
import csv
import io
from io import StringIO
from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse
from mangum import Mangum

class AdobeAssessment():
    def __init__(self):
        self.df = pd.DataFrame()
        self.groupedResults = pd.DataFrame()
        self.externalSearchEngineRevenue = pd.DataFrame(columns=['IP', 'SearchEngine', 'Keyword', 'Revenue'])
        s3_client =boto3.client('s3')
        self.s3 = boto3.resource('s3',aws_access_key_id= 'AKIAYS5QNA75B6KMHI3O', aws_secret_access_key='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')

    def initializeDataframe(self, s3_bucket_name, file):
        #s3_bucket_name='adobeassessment'

        #file = 'input/data[4][63][84][3] (1).tsv'
        obj = self.s3.Object(s3_bucket_name,file)

        df = []

        data=obj.get()['Body'].read()

        s=str(data,'utf-8')

        data = StringIO(s) 

        self.df = pd.read_csv(data, sep='\t')
        
    def calculateExternalSearchEngineRevenue(self):
        uniqueIPs = self.df['ip'].unique()
        
        i = 0

        for ip in uniqueIPs:
            sessionDF = self.df.loc[self.df['ip'].str.contains(ip)]
            if (sessionDF['pagename'].str.contains("Order Complete")).any():
                pd.to_datetime(sessionDF['date_time'])
                sessionDF.sort_values(by = ['date_time'])

                if "www.esshopzilla.com" not in sessionDF['referrer'].iloc[0]:
                    url = sessionDF['referrer'].iloc[0]
                    domain = urlparse(url).netloc
                    url_query = urlparse(url).query
                    search = re.search('q=(.+?)&', url_query).group(1)

                    if '+' in search:
                        keywords = search.split('+')
                        search = ' '.join(keywords)

                    product_list = sessionDF.loc[sessionDF['pagename'].str.contains("Order Complete"), ['product_list']]
                    product_list = product_list.values.tolist()
                    product_list = product_list[0][0]
                    product_list_array = product_list.split(';')
                    revenue = product_list_array[3]

                    # The list to append as row
                    ls = [ip, domain, search, int(revenue)]
                    # Create a pandas series from the list
                    row = pd.Series(ls, index= self.externalSearchEngineRevenue.columns)
                    self.externalSearchEngineRevenue = self.externalSearchEngineRevenue.append(row, ignore_index=True)

        self.groupedResults = self.externalSearchEngineRevenue.groupby('SearchEngine')['Revenue'].sum().reset_index().sort_values(by = ['Revenue'], ascending= False)
        

    def dumpResult(self):
        csv_buffer1 = StringIO()
        self.externalSearchEngineRevenue.to_csv(csv_buffer1, index = False)
        self.s3.Object('adobeassessment', 'output/externalSearchEngineRevenue.csv').put(Body=csv_buffer1.getvalue())
        csv_buffer2 = StringIO()
        self.groupedResults.to_csv(csv_buffer2, index = False)
        self.s3.Object('adobeassessment', 'output/grouped_result.csv').put(Body=csv_buffer2.getvalue())



#driver code
app = FastAPI(docs_url=None, redoc_url=None, openapi_url="/openapi.json")

#app = FastAPI(root_path="/dev")  
@app.get("/docs", tags=["Documentation"])
async def get_documentation():
    response = get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
    return response

@app.get("/dumpResults", tags=["dump external search engine revenue results"])
async def dumpResults(bucketname: str, path: str):
    o = AdobeAssessment()
    o.initializeDataframe(bucketname, path)
    o.calculateExternalSearchEngineRevenue()
    o.dumpResult()
    return "Pipeline Completed"

handler = Mangum(app)