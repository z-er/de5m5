FROM python:3.11.3

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt
ENTRYPOINT ["uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8000"]