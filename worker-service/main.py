from typing import Dict, Optional
import pika
import json
import mysql.connector
import os
from datetime import datetime
import time

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

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"Database connection failed: {str(e)}")
        return None

def update_task_status(task_id: str, status: str, completed_at: Optional[datetime] = None):
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    try:
        if completed_at:
            update_query = """
                UPDATE tasks 
                SET status = %s, completed_at = %s, updated_at = NOW()
                WHERE id = %s
            """
            cursor.execute(update_query, (status, completed_at, task_id))
        else:
            update_query = """
                UPDATE tasks 
                SET status = %s, updated_at = NOW()
                WHERE id = %s
            """
            cursor.execute(update_query, (status, task_id))
        
        conn.commit()
        return cursor.rowcount > 0
    except mysql.connector.Error as e:
        print(f"Failed to update task status: {str(e)}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def process_task(task_data: Dict):
    """
    Process a task asynchronously
    """
    task_id = task_data.get('task_id')
    
    if not task_id:
        print("[Worker] Missing task_id in message")
        return False
    
    try:
        print(f"[Worker] Processing task: {task_id}")
        
        # Update status to PROCESSING (optional)
        update_task_status(task_id, 'PROCESSING')
        
        # Simulate work (5-10 seconds)
        processing_time = 5
        print(f"[Worker] Simulating work for {processing_time} seconds...")
        time.sleep(processing_time)
        
        # Update status to COMPLETED
        completed_at = datetime.now()
        if update_task_status(task_id, 'COMPLETED', completed_at):
            print(f"[Worker] Task {task_id} completed successfully at {completed_at}")
            return True
        else:
            print(f"[Worker] Failed to update task {task_id} status")
            return False
    
    except Exception as e:
        print(f"[Worker] Error processing task {task_id}: {str(e)}")
        return False

def callback(ch, method, properties, body):
    """
    Callback function for RabbitMQ message consumption
    """
    try:
        task_data = json.loads(body.decode('utf-8'))
        
        # Process the task
        success = process_task(task_data)
        
        if success:
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[Worker] Message acknowledged for task {task_data.get('task_id')}")
        else:
            # Negative acknowledgment - message will be requeued
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            print(f"[Worker] Message NACKed for task {task_data.get('task_id')}")
    
    except json.JSONDecodeError as e:
        print(f"[Worker] Invalid JSON in message: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    except Exception as e:
        print(f"[Worker] Unexpected error in callback: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_worker():
    """
    Start the worker service and begin consuming messages
    """
    print("[Worker] Starting worker service...")
    
    # Connect to RabbitMQ
    try:
        credentials = pika.PlainCredentials(RABBITMQ_CONFIG["user"], RABBITMQ_CONFIG["password"])
        parameters = pika.ConnectionParameters(host=RABBITMQ_CONFIG["host"], credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare queue (durable)
        channel.queue_declare(queue='task_queue', durable=True)
        
        # Set prefetch count to 1 (process one message at a time)
        channel.basic_qos(prefetch_count=1)
        
        # Consume messages
        print("[Worker] Waiting for messages. To exit press CTRL+C")
        channel.basic_consume(queue='task_queue', on_message_callback=callback)
        
        # Start consuming
        channel.start_consuming()
    
    except KeyboardInterrupt:
        print("\n[Worker] Worker stopped by user")
    
    except Exception as e:
        print(f"[Worker] Fatal error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    start_worker()
