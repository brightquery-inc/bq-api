from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
import jwt
from datetime import datetime, timedelta
from pydantic import BaseModel


app = FastAPI()

# Generate a random secret key to sign the JWT tokens.
SECRET_KEY = "viSVnH99NkoAV7J"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing and verification configuration.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    
# Passwords to be hashed
passwords_to_hash = ["password123", "testpass", "securepassword"]

# Dictionary to store test user data
fake_users_db = {}


import psycopg2
import datetime

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='bq_terminal',
    user='postgres',
    password='qss2023',
)

cur = conn.cursor()



now = datetime.datetime.now()

hashed_password = pwd_context.hash("Vakster@bright_query123")
# username = password.replace("password", "user")  # Just creating some unique usernames
username = "vak@sambath.com"  # Just creating some unique usernames
query =  "INSERT INTO public.temp_users (email, pwd, first_name, last_name, created_on, last_login) VALUES (%s, %s, %s, %s, %s, %s);"
data = (username, hashed_password, 'Vakster', '', now, now)
cur.execute(query, data)
conn.commit()
# query =  "select * from public.temp_users;"
# cur.execute(query)
# print(cur.fetchall())
# conn.commit()
    
    
cur.close()
conn.close()