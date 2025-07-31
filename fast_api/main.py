"""
FastAPI is a Python web framework for building APIs.
It's built on top of the standard library and provides a simple way to create RESTful APIs.

Here's an example of how you can use FastAPI:
"""

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello World!"}