from fastapi import FastAPI, HTTPException, Request, status, Depends, APIRouter, Header, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
# from fastapi import FastAPI, Request, 
from fastapi.security import OAuth2AuthorizationCodeBearer, OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse  
from fastapi import BackgroundTasks
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
from utils import log_to_cloudwatch, process_request
from fastapi.responses import FileResponse
import logging
from time import strftime
import config
from cloudwatch import cloudwatch
import logging
from config import *
from time import gmtime, strftime
from random import randint
from multiprocess import Pool
from database_activities import *
from cache_activities import *


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

def create_log_stream_name():
    return strftime("%Y%m%d%H%M%S", gmtime())+ str(random_with_N_digits(8))

app = FastAPI()
security = HTTPBasic()
db = DatabaseManager()
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

@app.post("/api/org")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_ID_API'
    search_universe='org'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/api/le")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_ID_API'
    search_universe='le'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/api/officers")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_ID_API'
    search_universe='officers'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/org")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_APPEND_API'
    search_universe='org'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/org-new")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_APPEND_API'
    search_universe='org'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            'append/org-new',
            request
        )
        if cached_response:

            return JSONResponse(content=cached_response)
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                'append/org-new',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/le")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_APPEND_API'
    search_universe='le'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/append/location")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_APPEND_LOCATION_API'
    search_universe='location'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])  

@app.post("/append/executives")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_APPEND_EXECUTIVES_API'
    search_universe='executives'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/bi/org")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_BUSINESS_IDENTITY_API'
    search_universe='org'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

@app.post("/bi/le")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_BUSINESS_IDENTITY_API'
    search_universe='le'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])


@app.post("/bi/location")
async def custom_link(request: Request, background_tasks: BackgroundTasks, user_email: str = Depends(get_current_username)):
    log_stream_name = create_logger()
    cache = TwoLevelCache(db.redis)    
    background_tasks.add_task(log_to_cloudwatch, f'log_stream_name: {log_stream_name}', 'info')        
    search_product='BQ_BUSINESS_IDENTITY_LOCATION_API'
    search_universe='location'
    background_tasks.add_task(log_to_cloudwatch, f'user_email: {user_email}', 'info')    
    background_tasks.add_task(log_to_cloudwatch, f'Product:{search_product} search universe:{search_universe}', 'info')            
    
    final_response={}
    payload_log={}
    try:
        request = await request.json()
        payload_log = request
    except Exception as e:        
        background_tasks.add_task(logger.error({"Error":"Invalid payload format"}))
        return JSONResponse(content = {"error":"Invalid payload format"}, status_code=400)    
    
    error = utils.validate_fields(request, search_product, search_universe)
    if error:
        background_tasks.add_task(log_to_cloudwatch,f'Error: {error}' , 'error')        
        
    if error is None:
        request = utils.field_mapping(request, search_product, search_universe)
        cached_response = await cache.get_cached_response(
            f'{search_product}/{search_universe}',
            request
        )
        if cached_response:
            background_tasks.add_task(log_to_cloudwatch,f'final response: {cached_response}' , 'info')        
            return JSONResponse(content =cached_response, status_code=cached_response['status'])
        else:
            background_tasks.add_task(log_to_cloudwatch, f'request::{request}', 'info')    
            req_query    =[]
            for field, query in request.items():
                if field !='search_universe':
                    req_dict = {field:query, 'sp':search_product, 'su':search_universe}            
                    req_query.append(req_dict)        
            background_tasks.add_task(log_to_cloudwatch, f'req_query::{req_query}', 'info')            

            with Pool() as pool:  # Adjust number of processes as needed
                final_response =pool.map(process_request, req_query)
                pool.close()
                pool.join()        
            response_ =  utils.merge_responses(final_response, search_universe,search_product, payload_log,is_test=False)
            await cache.set_cached_response(
                f'{search_product}/{search_universe}',
                request,
                response_,
                expire=CACHE_LIMIT  # 5 minutes
            )
            background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'info')        
    else:
        response_ = {"response":error,"status":400}
        background_tasks.add_task(log_to_cloudwatch,f'final response: {response_}' , 'error')        

    return JSONResponse(content =response_, status_code=response_['status'])

# uvicorn api:app --reload --host 0.0.0.0 --port 5001 --workers 4
# nohup uvicorn api:app --reload --host 0.0.0.0 --port 5000 &
# cd /home/ubuntu/terminal/Backend/Operations; source /home/ubuntu/terminal/Backend/Operations/venv/bin/activat