from fastapi import FastAPI, HTTPException, Request, status, Depends, APIRouter, Header, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
# from fastapi import FastAPI, Request, 
from fastapi.security import OAuth2AuthorizationCodeBearer, OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse  
from passlib.context import CryptContext 
import os, psutil, jwt, json, re
from typing import Optional, Union
from pydantic import BaseModel
import psycopg2
import requests
from psycopg2 import OperationalError
import datetime
from datetime import timedelta
import pandas as pd
import utils, search2utils, screenerutils
from fastapi.responses import FileResponse
import logging
from time import strftime
import config
from cloudwatch import cloudwatch
import logging
from config import *
from time import gmtime, strftime
from random import randint


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

def create_log_stream_name():
    return strftime("%Y%m%d%H%M%S", gmtime())+ str(random_with_N_digits(8))

app = FastAPI()
security = HTTPBasic()
logging.basicConfig(level=logging.INFO, filename='terminal2_and_search2.log')
logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler) 

def create_logger():
    stream_name = create_log_stream_name()

    handler = cloudwatch.CloudwatchHandler(
    log_group = config.LOG_GROUP, 
    log_stream = stream_name,
    access_id = config.ACCESS_ID, 
    access_key = config.ACCESS_TOKEN,
    region = config.REGION
    )

    handler.setFormatter(formatter)
    #Set the level
    logger.setLevel(logging.DEBUG)
    #Add the handler to the logger
    logger.addHandler(handler)
    return stream_name

handler = cloudwatch.CloudwatchHandler(
 log_group = config.LOG_GROUP,
 access_id = config.ACCESS_ID, 
 access_key = config.ACCESS_TOKEN,
 region = config.REGION
)

handler.setFormatter(formatter)
#Set the level
logger.setLevel(logging.DEBUG)
#Add the handler to the logger
logger.addHandler(handler)

# for temp user
SECRET_KEY = "viSVnH99NkoAV7J"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing and verification configuration.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# For Search/IR

db_config = {
    "host":'localhost',
    "port":5432,
    "dbname":'bq_terminal',
    "user":'postgres',
    "password":'qss2023',
}


def authenticate_user(username: str, password: str):
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM public.temp_users WHERE email = '{username}';")
        rows = cur.fetchone()
        if rows:
            if username == rows[1]:
                user = User(username=rows[1], hashed_password=rows[2])
                if user.verify_password(password):
                    return user
                else:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Invalid password"
                    )
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Email is not registered"
            )
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while connecting to the database"
        )
    finally:
        cur.close()
        conn.close()

def create_access_token(data: dict, expires_delta: timedelta = None):
    try:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.datetime.utcnow() + expires_delta
        else:
            expire = datetime.datetime.utcnow() + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error encoding the JWT token"
        )

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):    
    return credentials.username

origins = [
    "http://127.0.0.1:19071/",
    "http://0.0.0.0:19071/",
    "http://localhost:19071/",
    "http://0.0.0.0:8080/",
    "http://localhost:8080/", 
    "http://0.0.0.0:5000/",
    "http://54.184.241.45:8000/",
    "http://54.184.241.45:8080/",
    'http://localhost:3000/',
    "http://0.0.0.0:3000/",
    'http://d3ulza75ahkult.cloudfront.net',
    'https://d3ulza75ahkult.cloudfront.net',
    'http://bq-terminal-155995424.us-west-2.elb.amazonaws.com',
    'https://bq-terminal-155995424.us-west-2.elb.amazonaws.com',
    "http://35.93.115.246:8000/",
    "http://35.93.115.246:8080/",
    "http://52.35.22.61:8000/",
    "http://52.35.22.61:8080/",
    'http://bq-terminal-155995424.us-west-2.elb.amazonaws.com',
    '*'
]

app.add_middleware(
CORSMiddleware,
allow_origins=origins,
allow_credentials=True,
allow_methods=["*"],
allow_headers=["*"],
)

# client_id="78qhrqs3xou0i2"
# client_secret="YCdA9OM0Zpzvsr9z"

# os.environ['LINKEDIN_CLIENT_ID'] = client_id
# os.environ['LINKEDIN_CLIENT_SECRET'] = client_secret
        
# load_dotenv()

VESPA_ENDPOINT = "http://localhost:8080"

def remove_and_from_end(input_string):
    if input_string.lower().rstrip().endswith('and'):
        return input_string[:-3].rstrip()
    elif input_string.lower().rstrip().endswith('or'):
        return input_string[:-3].rstrip()
    else:
        return input_string



@app.get("/test")
async def index():
   return {"message": "Hello World test"}

@app.get("/api/custom_link")
def custom_link(query=None, yql=None, type='all', filter=None, ranking='bm25', hits=100, limit=50, offset=0, orderby=None, isAsc=False, field=None, user_id=None, bq_organization_ticker=None, bq_organization_lei = None, bq_legal_entity_parent_status = None, bq_legal_entity_id = None, bq_organization_id = None, matrix=None, tab=None, ult_selection=None, user_level = 1, side_bar=False):
    matrix_mapping = {"Search_by_officers": utils.officer_details,
                        "search_by_ticker_prefix":utils.search_ticker_prefix,
                        "search_by_ticker_matches":utils.search_ticker_matches,
                        "search_by_address":utils.search_by_address,
                        "db_filters":utils.unique_values,
                        "Parent_Details":utils.parent_entity_details,
                        "sidebar":utils.side_bar,
                        "Org_Revenue_Employment":utils.get_organization_history,
                        "search":utils.search,
                        "get_financial_data":utils.get_financial_data,
                        "company_name":utils.company_name,
                        "officer_details":utils.officer_inside_company_details,
                        "screener_universal_search":screenerutils.screener_search,
                        "screener_unique_values":screenerutils.get_unique_values,
                        "stats":utils.stats,
                        "search_by_location_address":utils.search_by_location_address,
                        "search_by_bq_location_name":utils.search_by_bq_location_name,
                        }
    
    try:
        if matrix_mapping.get(matrix,None) != None:
            
            if matrix in ['officer_details',"Search_by_officers"] :
                response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, bq_organization_ticker, bq_organization_lei, bq_legal_entity_parent_status, bq_legal_entity_id, bq_organization_id)
            elif matrix in ["get_financial_data","Org_Revenue_Employment"]:
                response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, )
            elif matrix == "sidebar":
                response = matrix_mapping[matrix](tab, query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, ult_selection)
            elif matrix in ["search_by_address","search_by_location_address"]:
                response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "terminal",ult_selection)
            elif matrix == "screener_universal_search":
                response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "terminal",side_bar)
            elif matrix == "screener_unique_values":
                response = matrix_mapping[matrix](user_level)
            elif matrix == "stats":
                response = matrix_mapping[matrix](data)
            elif matrix in ["search_by_ticker_matches","search_by_address","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "terminal")
            elif matrix in ["search_by_ticker_prefix"]:
                response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, True, field, user_id, "terminal")
        else:
            response = {"response":"Invalid matrix used","status":400}
    except Exception as e:
        return JSONResponse(content = {"error":str(e)}, status_code=400)

        response = except_error

    return JSONResponse(content = response['response'], status_code=response['status'])
    # except Exception as e:
    #     return JSONResponse(content = {"error":"An error occured at our end","details":str(e)}, status_code=400)


@app.post("/api/org")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_ID_API'
    search_universe='org'
    logger.info(f'user_email: {user_email}') 
    logger.info(f'Product:{search_product} search universe:{search_universe}') 
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "search_by_ticker_prefix":utils.search_ticker_prefix,
                            "search_by_ticker_matches":utils.search_ticker_matches,
                            "search_by_address":utils.search_by_address,
                            "search":utils.search,
                            "company_name":utils.company_name_updated,
                            # "search_by_location_address":utils.search_by_location_address,
                            # "search_by_bq_location_name":utils.search_by_bq_location_name,
                            "Search_by_officers": utils.officer_details,
                            "officer_details":utils.officer_inside_company_details,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

        response_ =  utils.merge_responses(final_response, search_universe,search_product,payload_log, is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/api/le")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_ID_API'
    search_universe='le'
    logger.info(f'user_email: {user_email}')
    logger.info(f'Product:{search_product} search universe:{search_universe}') 
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "search_by_ticker_prefix":utils.search_ticker_prefix,
                            "search_by_ticker_matches":utils.search_ticker_matches,
                            "search_by_address":utils.search_by_address,
                            "search":utils.search,
                            "company_name":utils.company_name_updated,
                            "Search_by_officers": utils.officer_details,
                            "officer_details":utils.officer_inside_company_details,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log, is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/api/officers")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_ID_API'
    search_universe='officers'
    logger.info(f'user_email: {user_email}')
    logger.info(f'Product:{search_product} search universe:{search_universe}') 
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product,search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "Search_by_officers": utils.officer_details,
                            "officer_details":utils.officer_inside_company_details,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/org")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_APPEND_API'
    search_universe='org'
    logger.info(f'user_email: {user_email}')
    logger.info(f'Product:{search_product} search universe:{search_universe}')   
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "search_by_ticker_prefix":utils.search_ticker_prefix,
                            "search_by_ticker_matches":utils.search_ticker_matches,
                            "search_by_address":utils.search_by_address,
                            "search":utils.search,
                            "company_name":utils.company_name_updated,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        # print(field,'response:',response)

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/le")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_APPEND_API'
    search_universe='le'
    logger.info(f'user_email: {user_email}')     
    logger.info(f'Product:{search_product} search universe:{search_universe}')  
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "search_by_address":utils.search_by_address,
                            "search":utils.search,
                            "company_name":utils.company_name_updated,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

                    response = except_error

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/location")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_APPEND_LOCATION_API'
    search_universe='location'
    logger.info(f'user_email: {user_email}')     
    logger.info(f'Product:{search_product} search universe:{search_universe}')    
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]                
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                                "search_by_location_address":utils.search_by_location_address_updated,
                                "search_by_bq_location_name":utils.search_by_bq_location_name
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

                    response = except_error

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])    

@app.post("/append/executives")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_APPEND_EXECUTIVES_API'
    search_universe='executives'
    logger.info(f'user_email: {user_email}')     
    logger.info(f'Product:{search_product} search universe:{search_universe}')    
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]                
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                                "Search_by_executive": utils.Search_by_executive_updated,
                                "Search_by_executive_other": utils.search_executive_other,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name","Search_by_executive_other"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')
                        elif matrix in ["Search_by_executive",'officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

                    response = except_error

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/bi/org")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_BUSINESS_IDENTITY_API'
    search_universe='org'
    logger.info(f'user_email: {user_email}')
    logger.info(f'Product:{search_product} search universe:{search_universe}')   
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "search_by_ticker_prefix":utils.search_ticker_prefix,
                            "search_by_ticker_matches":utils.search_ticker_matches,
                            "search_by_address":utils.search_by_address,
                            "search":utils.search,
                            "company_name":utils.company_name_updated,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        # print(field,'response:',response)

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/bi/le")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_BUSINESS_IDENTITY_API'
    search_universe='le'
    logger.info(f'user_email: {user_email}')     
    logger.info(f'Product:{search_product} search universe:{search_universe}')  
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')        

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]
                if ('exact' in field) & ('ticker' in field):
                    field = field.split('_exact')[0]
                    matrix='search_by_ticker_matches'
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                            "search_by_address":utils.search_by_address,
                            "search":utils.search,
                            "company_name":utils.company_name_updated,
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

                    response = except_error

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])


@app.post("/bi/location")
async def custom_link(request: Request, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    logger.info(f'log_stream_name: {log_stream_name}')
    search_product='BQ_BUSINESS_IDENTITY_LOCATION_API'
    search_universe='location'
    logger.info(f'user_email: {user_email}')     
    logger.info(f'Product:{search_product} search universe:{search_universe}')    
    final_response={}
    try:
        request = await request.json()
        payload_log = request
        # print('request:', request)
        logger.info(f'request:{request}')        
    except Exception as e:
        logger.info({"Error":"Invalid payload format"})
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        logger.info(f'Error: {error}')

    if error is not None:
        response_ = {"response":error,"status":400}
    else:    
        request = utils.field_mapping(request, search_product, search_universe)
        logger.info(f'request_new: {request}')
        yql,field,user_id,tab,ult_selection,orderby,type,filter,ranking,hits,limit,offset,isAsc,user_level,side_bar,request,search_universe =utils.initialize_parameters(request)
        logger.info(f'request keys: {request.keys()} request values: {request.values()} ult_selection {ult_selection}, orderby: {orderby} isAsc:{isAsc}')

        for field, query in request.items():
            if query:
                print('search_universe', search_universe, 'field', field)
                matrix = FIELD_MATRIX_MAPPING[search_product][search_universe][field]                
                print('\nmatrix:', matrix, 'field:',field, 'query:',query,'\n')            
                matrix_mapping = {
                                "search_by_location_address":utils.search_by_location_address_updated,
                                "search_by_bq_location_name":utils.search_by_bq_location_name
                            }
                try:
                    if matrix_mapping.get(matrix,None) != None:                    
                        if matrix in ["search_by_ticker_matches","db_filters","Parent_Details","company_name","search", "search_by_bq_location_name"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product)
                        elif matrix in ["search_by_ticker_prefix"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, "external", search_product, user_id )
                        elif matrix in ["search_by_address","search_by_location_address"]:
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, user_id, "external",search_product,ult_selection)
                        elif matrix in ['officer_details',"Search_by_officers"] :
                            response = matrix_mapping[matrix](query, yql, type, filter, ranking, hits, limit, offset, orderby, isAsc, field, search_product, user_id, '', '', '', '', '')

                        final_response[field] = response['response']
                    else:
                        logger.info({"response":"Invalid matrix used","status":400})
                        response = {"response":"Invalid matrix used","status":400}
                except Exception as e:
                    return JSONResponse(content = {"error":str(e)}, status_code=400)

                    response = except_error

        response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
        logger.info(f'final response: {response_}')

    return JSONResponse(content =response_, status_code=response_['status'])

# uvicorn api:app --reload --host 0.0.0.0 --port 5000
# nohup uvicorn api:app --reload --host 0.0.0.0 --port 5000 &
# cd /home/ubuntu/terminal/Backend/Operations; source /home/ubuntu/terminal/Backend/Operations/venv/bin/activat