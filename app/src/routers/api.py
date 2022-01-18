from app.src.databases.database_methods import TableMethods
from app.src.databases.database_storage import Level1ExceptionObject, UpdateExceptionObject
from app.src.models.ormar_models import tbl_level0_alert_details, tbl_level1_exception_message_filtered, tbl_level2_frequent_exceptions, BaseMeta
from app.src.databases.database_schema import database, engine, metadata
from app.src.databases.data_cleansing import DataCleansing
from app.src.databases.helper import format_text
from app.src.databases.singleton import Singleton
from fastapi import FastAPI, Depends, Request
from sqlalchemy.orm import Session
from fastapi import FastAPI, Header, HTTPException, Body, Depends, Form
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import APIRouter
from pydantic import BaseModel
import logging
import pydantic
import asyncio
import asyncpg
import datetime
import requests
import threading
from threading import Thread
import configparser


"""
:file: 
This file contains logic to create fastapi endpoints

""" 


logging.basicConfig()
router = APIRouter()



class Item(BaseModel):

    informed_at: str 
    queue_name: str 
    queued_items_num: int 
    webhook_endpoint_url: str




templates = Jinja2Templates(directory = "app/src/templates")

class FastapiMethod():   
    
    def create_eventloop():
        """ Create event loop
            Useful to call for each data input iteration
        """

        try:
            return asyncio.get_event_loop()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return asyncio.get_event_loop()



    @router.post("/items/")
    async def run_pipeline(item: Item):
        """ Endpoint to store data into database
        :param item: Format data posted from webhook 
        """
        # Step0: Check queue length
        if item.queued_items_num <= 0:
            error_body = {"error message": "Incorrect number of items in queue"}
            return JSONResponse(content = error_body, status_code = 400)

        def update_database(**kwargs):
            loop = kwargs.get('loop', {})
            queued_items_num = item.queued_items_num
            webhook_endpoint_url = item.webhook_endpoint_url
            
            queue_get_multiple = []
            while(queued_items_num > 0):
                number_of_items = 3
                if queued_items_num < 3:
                    number_of_items = queued_items_num
                try:
                    # Step1: Get queue in webhook, 3 items at a time
                    params_get = {'multiple': 'true', 'limit': number_of_items}
                    request_get = requests.get(url = webhook_endpoint_url, params = params_get)
                    if request_get and 'json' in request_get.headers.get('Content-Type'):
                        logging.info('successfully received data from webhook')
                        queue_get_multiple = request_get.json()['data']
            
                        # Step2: cleanse and store data
                        try:
                            cleaned_queue_get_multiple = format_text(queue_get_multiple)
                            for each in cleaned_queue_get_multiple:
                                cleaned_each = DataCleansing(each).clean_data() 
                                if not cleaned_each.empty:                  
                                    loop.run_until_complete(UpdateExceptionObject().update_tables_during_day(cleaned_data = cleaned_each))
                                    logging.info('successfully stored data into database') 
                        except:
                            error_body = {"error message": "Failed to save data into database"}
                            return JSONResponse(content = error_body, status_code = 400)

                    # Step3: Pop queue in webhook 
                    try:
                        params_pop = {'popMultiple': 'true', 'limit': number_of_items}
                        request_pop_multiple = requests.get(url = webhook_endpoint_url, params = params_pop)
                        logging.info('Successfully popped %s items from queue', number_of_items)

                        if queued_items_num < 3:
                            queued_items_num = 0
                        else:
                            queued_items_num -= 3
                    except:
                        logging.info('Failed to pop multiple')
                except:
                        error_body = {"error message": "Failed to received, uploded and deleted data"}
                        return JSONResponse(content = error_body, status_code = 400)
        loop = FastapiMethod.create_eventloop()
        thread = threading.Thread(target=update_database, kwargs={'loop': loop})
        thread.start()
        message = {"message": "Successfully received, uploded and deleted data"}
        return JSONResponse(message, status_code = 200)



    @router.get('/')
    async def home(request: Request):
        """ Endpoint of home page
        :param: Request: HTML request
        """
        return templates.TemplateResponse("main_page.html", context = {'request': request})



    @router.get('/database/updatemomory')
    async def call_update_memory():
        """ Function to update database memory
        """
        try:
            UpdateExceptionObject().update_memory_job()
            return {"message": "Successfully called update_memory_job function"}
        except:
            return {"message": "Unable to call update_memory_job function"}



    @router.get('/database/endday')
    async def call_end_day():
        """ Function to update database day
        """

        try:
            UpdateExceptionObject().day_end_job()
            list_of_known_frequent_exceptions = UpdateExceptionObject().list_of_known_frequent_exceptions
            message = {"length of known frequent exceptions": len(list_of_known_frequent_exceptions), 
                        "data": list_of_known_frequent_exceptions}
            return message
        except:
            return {"message": "Unable to call day_end_job function"}



    @router.get('/database/getfrequentexceptions', response_class = HTMLResponse)
    async def get_frequent_exceptions(request: Request):
        """ Endpoint of get frequent exceptions
        :param: Request: HTML request
        """
        try:
            list_frequent_exceptions = UpdateExceptionObject().list_of_frequent_exceptions
            return templates.TemplateResponse("get_frequent_exceptions.html", 
                                context = {"request": request, 
                                "length of frequent exceptions": len(list_frequent_exceptions), 
                                "data": list_frequent_exceptions})
        except:
            return {"message": "Unable to call get_frequent_exceptions function"}



    @router.get('/database/getnewexceptions', response_class = HTMLResponse)
    async def get_new_exceptions(request: Request):
        """ Endpoint of get new exceptions
        :param: Request: HTML request
        """
        try:
            list_new_exceptions = UpdateExceptionObject().get_new_exceptions()
            return templates.TemplateResponse("get_new_exceptions.html", \
                                context = {"request": request, 
                                "length of frequent exceptions": len(list_new_exceptions), 
                                "data": list_new_exceptions})
        except:
            return {"message": "Unable to call get_new_exceptions function"}

    


    @router.get('/database/mark', response_class = HTMLResponse)
    async def mark_known_exceptions(request: Request):
        """ Endpoint of get known exceptions
        :param: Request: HTML request
        """
        list_exceptions = asyncio.run(TableMethods().get_all(tbl_level1_exception_message_filtered))
        return templates.TemplateResponse('mark_known_exceptions.html', 
                                        context = {'request': request, 
                                        "data": list_exceptions})



    @router.post('/items/submitform', response_class = HTMLResponse)
    async def handle_mark_known_exceptions(request: Request, 
                                            region: str = Form(...), 
                                            level1_exception_message_filtered: str = Form(...), 
                                            level2_exception_classes: str = Form(...)):
        """ Endpoint of mark known exceptions
        :param: Request: HTML request
        :param: region: Kibana region
        :param: level1_exception_message_filtered: level1 exception message
        :param: level2_exception_classes: level2 exception message
        """
        try:
            await TableMethods().modify_table1_add_table2(table1_name = tbl_level1_exception_message_filtered, 
                                                    table2_name = tbl_level2_frequent_exceptions, 
                                                    region = region,
                                                    level1_exception_message_filtered = level1_exception_message_filtered, 
                                                    level2_exception_classes = level2_exception_classes)
            
            message = {"message": "Successfully marked known exceptions"}
            return JSONResponse(message, status_code = 200)
        except:
            message = {"message": "Faled to mark known exceptions"}
            return JSONResponse(message, status_code = 200)



    @router.get('/database/parameters', response_class = HTMLResponse)
    async def get_parameters(request: Request):
        """ Endpoint of get application parameters
        :param: Request: HTML request
        """
        exception_percentile = UpdateExceptionObject().exception_percentile
        exception_days = UpdateExceptionObject().exception_days
        data = {'exception_percentile': exception_percentile, 'exception_days': exception_days}
        return templates.TemplateResponse('set_application_parameters.html', context = {'request': request, 'data': data})



    @router.post('/items/setparameters', response_class = HTMLResponse)
    async def set_parameters(request: Request, 
                            new_exception_percentile: int = Form(...), 
                            new_exception_days: int = Form(...)):
        """ Endpoint of reset application parameters
        :param: Request: HTML request
        :param: new_exception_percentile: user input percentile
        :param: new_exception_days: user input days
        """
        config = configparser.ConfigParser()
        config.read('app/config.ini')
        
        config.set('outage-parameters', "percentile", str(new_exception_percentile))
        config.set('outage-parameters', "days", str(new_exception_days))
        with open('app/config.ini', 'w') as configfile:
            config.write(configfile)

        UpdateExceptionObject().first_run()
        message = {"message": "Successfully reset application parameters", "new_exception_percentile": new_exception_percentile, "new_exception_days": new_exception_days}
        return JSONResponse(message, status_code = 200)