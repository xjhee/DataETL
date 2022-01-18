FROM python:3.7

EXPOSE 8000
WORKDIR /app/
COPY requirements.txt .
COPY . /app 

RUN pip3 install -r requirements.txt
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]