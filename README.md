# Asynchronous Task Processing API with Message Queues

## Project Description

This project implements a robust backend service that handles asynchronous task processing using RabbitMQ as a message queue. The system consists of:

- **API Service**: Accepts task submission requests via RESTful API endpoints
- **Worker Service**: Processes tasks asynchronously by consuming messages from RabbitMQ
- **MySQL Database**: Persists task metadata and status
- **Docker Compose**: Orchestrates all services for easy local development

## Features

- Task submission via `POST /tasks` endpoint
- Task status retrieval via `GET /tasks/{task_id}` endpoint
- Asynchronous task processing using RabbitMQ message queue
- Task status tracking (PENDING, PROCESSING, COMPLETED, FAILED)
- Containerized with Docker for consistent deployment

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Git

### Running the Application

1. Clone the repository:
```bash
git clone https://github.com/sarayu1201/-Asynchronous-Task-Processing-API-with-Message-Queues.git
cd -Asynchronous-Task-Processing-API-with-Message-Queues
```

2. Create `.env` file from `.env.example`:
```bash
cp .env.example .env
```

3. Build and start all services:
```bash
docker-compose up --build
```

4. The API will be available at `http://localhost:8000`

## API Documentation

### POST /tasks

Submit a new task for asynchronous processing.

**Request Body:**
```json
{
  "title": "Task Title",
  "description": "Task Description",
  "metadata": {
    "key": "value"
  }
}
```

**Response (202 Accepted):**
```json
{
  "task_id": "uuid-here",
  "message": "Task submitted successfully for processing"
}
```

### GET /tasks/{task_id}

Retrieve the status and details of a specific task.

**Response (200 OK):**
```json
{
  "task_id": "uuid-here",
  "status": "COMPLETED",
  "title": "Task Title",
  "description": "Task Description",
  "created_at": "2026-02-12T14:00:00",
  "updated_at": "2026-02-12T14:00:05",
  "completed_at": "2026-02-12T14:00:10"
}
```

## Technologies Used

- **Python** with FastAPI
- **RabbitMQ** for message queuing
- **MySQL** for data persistence
- **Docker** for containerization
- **Pika** for RabbitMQ client
- **MySQL Connector** for database access

## Project Structure

```
.
├── api-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── worker-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── db/
│   └── init.sql
├── docker-compose.yml
└── .env.example
```

## Database Schema

The `tasks` table stores task metadata with the following columns:

- `id`: UUID (Primary Key)
- `title`: Task title (VARCHAR)
- `description`: Task description (TEXT)
- `status`: Task status (ENUM: PENDING, PROCESSING, COMPLETED, FAILED)
- `metadata`: Additional task data (JSON)
- `created_at`: Timestamp when task was created
- `updated_at`: Timestamp when task was last updated
- `completed_at`: Timestamp when task was completed

## How It Works

1. Client submits a task via `POST /tasks`
2. API service validates input, generates UUID, saves task to MySQL with status 'PENDING'
3. API publishes task details to RabbitMQ queue
4. Worker service consumes the message from RabbitMQ
5. Worker simulates task processing (5-10 seconds delay)
6. Worker updates task status to 'COMPLETED' in MySQL
7. Client can check task status via `GET /tasks/{task_id}`

## Error Handling

The application implements comprehensive error handling:

- Input validation for required fields
- Database connection error handling
- Message queue connection error handling
- Standardized JSON error responses
- Appropriate HTTP status codes (400, 404, 500)

## Environment Variables

See `.env.example` for required environment variables.

## Testing

Unit and integration tests are provided in the `tests/` directory. Run tests with:

```bash
docker-compose exec api_service pytest
docker-compose exec worker_service pytest
```

## Future Enhancements

- Add authentication and authorization
- Implement rate limiting
- Add monitoring and alerting
- Implement retry logic with exponential backoff
- Add dead-letter queue for failed messages
- Implement task prioritization

## License

This project is licensed under the MIT License.

## Author

Sarayu Allampalli
