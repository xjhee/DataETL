import nest_asyncio
nest_asyncio.apply()
from app.src.databases.database_storage import Level1ExceptionObject, UpdateExceptionObject
from app.src.databases.database_schema import database, engine, metadata
from app.src.routers.api import router
from configparser import ConfigParser 
from fastapi import FastAPI
import requests
import logging

"""
:file: 
This file contains main file to serve the application

""" 


app = FastAPI()
app.include_router(router)


@app.on_event("startup")
async def startup():
    """ Start application process
    """
    await database.connect()
    try:
        UpdateExceptionObject().first_run()
        logging.info("Successfully called first_run function") 
    except:
        logging.info('Failed to call first_run function')



@app.on_event("shutdown")
async def shutdown():
    """ Shut down application process
    """
    await database.disconnect()






if __name__ == "__main__":
    pass
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
