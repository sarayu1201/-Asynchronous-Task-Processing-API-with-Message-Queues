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
    """Establish and return a MySQL database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"[Worker] Database connection failed: {str(e)}")
        return None

def update_task_status(task_id: str, status: str, completed_at: Optional[datetime] = None):
    """Update task status in the database."""
    conn = get_db_connection()
    if not conn:
        print(f"[Worker] Cannot update status for task {task_id}: No database connection")
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
        rows_affected = cursor.rowcount
        if rows_affected > 0:
            print(f"[Worker] Task {task_id} status updated to '{status}'")
        else:
            print(f"[Worker] Warning: Task {task_id} not found in database")
        return rows_affected > 0
    except mysql.connector.Error as e:
        print(f"[Worker] Failed to update task {task_id} status: {str(e)}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def process_task(task_data: Dict):
    """
    Process a task asynchronously.
    Extracts task details, simulates processing, and updates task status.
    """
    task_id = task_data.get('task_id')
    title = task_data.get('title', 'N/A')
    description = task_data.get('description', 'N/A')
    metadata = task_data.get('metadata', {})
    
    if not task_id:
        print("[Worker] Error: Missing task_id in message")
        return False
    
    try:
        print(f"[Worker] Processing task: {task_id}")
        print(f"[Worker] Title: {title}")
        print(f"[Worker] Description: {description}")
        if metadata:
            print(f"[Worker] Metadata: {json.dumps(metadata)}")
        
        # Update status to PROCESSING (optional but good for tracking)
        update_task_status(task_id, 'PROCESSING')
        
        # Simulate work (5-10 seconds)
        processing_time = 5
        print(f"[Worker] Simulating work for {processing_time} seconds...")
        time.sleep(processing_time)
        
        # Update status to COMPLETED with timestamp
        completed_at = datetime.now()
        if update_task_status(task_id, 'COMPLETED', completed_at):
            print(f"[Worker] Task {task_id} completed successfully at {completed_at.isoformat()}")
            return True
        else:
            print(f"[Worker] Failed to update task {task_id} status to COMPLETED")
            return False
        
    except Exception as e:
        print(f"[Worker] Error processing task {task_id}: {str(e)}")
        try:
            update_task_status(task_id, 'FAILED')
        except Exception as update_error:
            print(f"[Worker] Could not update task status to FAILED: {str(update_error)}")
        return False

def callback(ch, method, properties, body):
    """
    Callback function for RabbitMQ message consumption.
    Processes each message and acknowledges or negatively acknowledges based on result.
    """
    try:
        task_data = json.loads(body.decode('utf-8'))
        print(f"[Worker] Received message for task: {task_data.get('task_id', 'Unknown')}")
        
        # Process the task
        success = process_task(task_data)
        
        if success:
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[Worker] Message acknowledged for task {task_data.get('task_id')}")
        else:
            # Negative acknowledgment - message will be requeued
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            print(f"[Worker] Message NACKed for task {task_data.get('task_id')} - will be retried")
        
    except json.JSONDecodeError as e:
        print(f"[Worker] Error: Invalid JSON in message: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
    except Exception as e:
        print(f"[Worker] Unexpected error in callback: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_worker():
    """
    Start the worker service and begin consuming messages from RabbitMQ.
    """
    print("[Worker] Starting worker service...")
    print(f"[Worker] Connecting to RabbitMQ at {RABBITMQ_CONFIG['host']}...")
    
    # Connect to RabbitMQ
    try:
        credentials = pika.PlainCredentials(RABBITMQ_CONFIG["user"], RABBITMQ_CONFIG["password"])
        parameters = pika.ConnectionParameters(host=RABBITMQ_CONFIG["host"], credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        print("[Worker] Successfully connected to RabbitMQ")
        
        # Declare queue (durable to survive broker restarts)
        channel.queue_declare(queue='task_queue', durable=True)
        print("[Worker] Queue 'task_queue' declared")
        
        # Set prefetch count to 1 (process one message at a time)
        channel.basic_qos(prefetch_count=1)
        print("[Worker] Prefetch count set to 1")
        
        # Consume messages
        print("[Worker] Waiting for messages. To exit press CTRL+C")
        channel.basic_consume(queue='task_queue', on_message_callback=callback)
        
        # Start consuming
        channel.start_consuming()
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[Worker] RabbitMQ connection error: {str(e)}")
        print("[Worker] Make sure RabbitMQ is running and accessible")
        exit(1)
    except KeyboardInterrupt:
        print("\n[Worker] Worker stopped by user")
        
    except Exception as e:
        print(f"[Worker] Fatal error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    start_worker()
