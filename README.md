# RunSurge-Master

A distributed computing platform that manages and executes tasks across multiple worker nodes.

## Requirments

1. `python 3.11.9`
2. `postgress`

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with the following configuration:
```env
# Security
SECRET_KEY=your-secret-key-here
ALGORITHM=HS256 (optional, default=HS256)
ACCESS_TOKEN_EXPIRE_MINUTES=60 (optional, default=60)
REFRESH_TOKEN_EXPIRE_DAYS=7 (optional, default=7)

# Database
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_NAME=runsurge
DB_PORT=5432

#Master
MASTER_PORT=8000 (optional, default=8000)
```

4. Run the application:
```bash
python main.py
```

The API will be available at `http://localhost:8000`

## API Documentation

The API documentation is available through Swagger UI and ReDoc:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI Schema: `http://localhost:8000/openapi.json`

## Configuration

All configuration settings are managed through the `Settings` class in `app/core/config.py`. This class handles:
- Security settings (JWT tokens, secrets)
- Database connection parameters
- Environment-specific configurations

## API Examples

### Authentication

```python
# Login
POST /api/auth/login
{
    "username_or_email": "user123",
    "password": "password123"
}

# Register
POST /api/auth/register
{
    "username": "newuser",
    "email": "user@example.com",
    "password": "password123"
}

# Refresh Token
POST /api/auth/refresh
{
    "refresh_token": "your-refresh-token"
}
```

### User Endpoints

```python
# Get current user (requires authentication)
GET /api/users/me
Headers: Authorization: Bearer your-access-token

# Example response:
{
    "user": {
        "id": 1,
        "username": "user123",
        "email": "user@example.com"
    }
}
```

## Project Structure

```
RunSurge-Master/
├── app/
│   ├── api/          # API endpoints
│   ├── core/         # Core functionality
│   ├── db/           # Database models and repositories
│   └── schemas/      # Pydantic models
├── main.py           # Application entry point
└── .env             # Environment configuration
```