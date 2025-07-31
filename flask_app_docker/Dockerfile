FROM python:3.11.3

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt
ENTRYPOINT ["python", "flask_app.py"]