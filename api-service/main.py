from fastapi import FastAPI, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional, Dict
import uuid
import json
import mysql.connector
import pika
import os
from datetime import datetime

app = FastAPI(title="Asynchronous Task Processing API")

# Database configuration
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql_db"),
    "user": os.getenv("MYSQL_USER", "user"),
    "password": os.getenv("MYSQL_PASSWORD", "password"),
    "database": os.getenv("MYSQL_DATABASE", "async_tasks_db")
}

# RabbitMQ configuration
RABBITMQ_CONFIG = {
    "host": os.getenv("RABBITMQ_HOST", "rabbitmq"),
    "user": os.getenv("RABBITMQ_USER", "guest"),
    "password": os.getenv("RABBITMQ_PASSWORD", "guest")
}

# Models

class TaskCreate(BaseModel):
    title: str
    description: str
    metadata: Optional[Dict] = None

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    title: str
    description: str
    created_at: str
    updated_at: str
    completed_at: Optional[str] = None

class ErrorResponse(BaseModel):
    detail: str

# Database helper

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def create_task_in_db(task_id: str, title: str, description: str, metadata: Optional[Dict]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        insert_query = """
            INSERT INTO tasks (id, title, description, status, metadata)
            VALUES (%s, %s, %s, 'PENDING', %s)
        """
        cursor.execute(insert_query, (task_id, title, description, json.dumps(metadata or {})))
        conn.commit()
        return True
    except mysql.connector.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create task: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def get_task_from_db(task_id: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        select_query = "SELECT * FROM tasks WHERE id = %s"
        cursor.execute(select_query, (task_id,))
        task = cursor.fetchone()
        if not task:
            return None
        return task
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve task: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# RabbitMQ helper
def publish_to_queue(task_data: Dict):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_CONFIG["user"], RABBITMQ_CONFIG["password"])
        parameters = pika.ConnectionParameters(host=RABBITMQ_CONFIG["host"], credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        channel.queue_declare(queue='task_queue', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(task_data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        
        channel.close()
        connection.close()
        return True
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish to queue: {str(e)}")

# API Endpoints

@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED, response_model=dict, responses={400: {"model": ErrorResponse}})
def submit_task(task: TaskCreate):
    """
    Submit a new task for asynchronous processing
    """
    
    # Generate unique task_id
    task_id = str(uuid.uuid4())
    
    # Create task in database
    create_task_in_db(
        task_id=task_id,
        title=task.title,
        description=task.description,
        metadata=task.metadata
    )
    
    # Publish to RabbitMQ
    message = {
        'task_id': task_id,
        'title': task.title,
        'description': task.description,
        'metadata': task.metadata
    }
    publish_to_queue(message)
    
    return {
        'task_id': task_id,
        'message': 'Task submitted successfully for processing'
    }

@app.get("/tasks/{task_id}", response_model=TaskStatusResponse, responses={404: {"model": ErrorResponse}})
def get_task_status(task_id: str):
    """
    Retrieve the current status of a specific task
    """
    
    task = get_task_from_db(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return TaskStatusResponse(
        task_id=task['id'],
        status=task['status'],
        title=task['title'],
        description=task['description'],
        created_at=task['created_at'].isoformat(),
        updated_at=task['updated_at'].isoformat(),
        completed_at=task['completed_at'].isoformat() if task['completed_at'] else None
    )

@app.get("/", response_model=dict)
def root():
    """
    Root endpoint
    """
    return {"message": "Asynchronous Task Processing API is running"}

@app.get("/health", response_model=dict)
def health():
    """
    Health check endpoint
    """
    try:
        # Test database connection
        conn = get_db_connection()
        conn.close()
        
        # Test RabbitMQ connection
        credentials = pika.PlainCredentials(RABBITMQ_CONFIG["user"], RABBITMQ_CONFIG["password"])
        parameters = pika.ConnectionParameters(host=RABBITMQ_CONFIG["host"], credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        connection.close()
        
        return {"status": "healthy", "database": "connected", "rabbitmq": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
