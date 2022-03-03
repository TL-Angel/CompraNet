# -*- coding: utf-8 -*-
__author__ = "Oscar López"
__copyright__ = "Copyright 2021, Literata"
__credits__ = ["Oscar López"]
__license__ = "GPL"
__version__ = "1.0.0"
__email__ = "lpz.oscr@gmail.com"
__status__ = "Development"

from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import Depends, HTTPException, status, Security
from fastapi.security import (OAuth2PasswordBearer, OAuth2PasswordRequestForm,
                              SecurityScopes)
from pydantic import BaseModel, ValidationError
from jose import JWTError, jwt
from passlib.context import CryptContext


# to get a string like this run:
# openssl rand -hex 32
# SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
SECRET_KEY = '727fcc3cabdf3ab99643472edc0ecca61ede71d98aefed8af13a5a038e534ce5'
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 60


fake_users_db = {
    'ti_uniclick': {
        'username': 'ti_uniclick',
        # 'full_name': 'John Doe',
        # 'email': 'johndoe@example.com',
        'hashed_password': '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW',
        'disabled': False,
    },
    'oscar': {
        'username': 'oscar',
        # 'full_name': 'Oscar Lópex',
        # 'email': 'ol@example.com',
        'hashed_password': '$2b$12$TCI2Y7xzKdzuAclwqBColuXo06/RCu5XXyJKhvV.Oev.lH.2CAmni',
        'disabled': False,
        }
}


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: List[str] = []


class User(BaseModel):
    username: str
    # email: Optional[str] = None
    # full_name: Optional[str] = None
    disabled: Optional[bool] = None


class UserInDB(User):
    hashed_password: str


PWD_CONTEXT = CryptContext(schemes=['bcrypt'], deprecated='auto')

OAUTH2_SCHEME = OAuth2PasswordBearer(
    tokenUrl='token',
    scopes={'me': 'Read information about the current user.',
            'items': 'Read items.'},
    )

def verify_password(plain_password, hashed_password):
    return PWD_CONTEXT.verify(plain_password, hashed_password)

def get_password_hash(password):
    return PWD_CONTEXT.hash(password)

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

def authenticate_user(fake_db, username: str, password: str):
    user = get_user(fake_db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta]=None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({'exp': expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(
    security_scopes: SecurityScopes, token: str = Depends(OAUTH2_SCHEME)
):
    if security_scopes.scopes:
        authenticate_value = f'Bearer scope="{security_scopes.scope_str}"'
    else:
        authenticate_value = f'Bearer'
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Could not validate credentials',
        headers={'WWW-Authenticate': authenticate_value},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get('sub')
        if username is None:
            raise credentials_exception
        token_scopes = payload.get('scopes', [])
        token_data = TokenData(scopes=token_scopes, username=username)
    except (JWTError, ValidationError):
        raise credentials_exception
    user = get_user(fake_users_db, username=token_data.username)
    if user is None:
        raise credentials_exception
    for scope in security_scopes.scopes:
        if scope not in token_data.scopes:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='Not enough permissions',
                headers={'WWW-Authenticate': authenticate_value},
            )
    return user


async def get_current_active_user(
    current_user: User=Security(get_current_user, scopes=['me'])
    ):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail='Inactive user')
    return current_user


# @app.post('/token', response_model=Token)
# async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
#     user = authenticate_user(fake_users_db, form_data.username, form_data.password)
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Incorrect username or password",
#             headers={"WWW-Authenticate": "Bearer"},
#         )
#     access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
#     access_token = create_access_token(
#         data={"sub": user.username}, expires_delta=access_token_expires
#     )
#     return {"access_token": access_token, "token_type": "bearer"}
#
#
# @app.get("/users/me/", response_model=User)
# async def read_users_me(current_user: User = Depends(get_current_active_user)):
#     return current_user
#
#
# @app.get("/users/me/items/")
# async def read_own_items(current_user: User = Depends(get_current_active_user)):
#     return [{"item_id": "Foo", "owner": current_user.username}]


if __name__ == '__main__':

    pass
