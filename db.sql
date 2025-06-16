-- Step 1: Create ENUM types
CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed');
CREATE TYPE data_location_type AS ENUM ('master', 'node');
CREATE TYPE task_status AS ENUM ('pending', 'running', 'completed', 'failed');
CREATE TYPE log_event_type AS ENUM ('started', 'completed', 'failed', 'ping');
CREATE TYPE payment_status AS ENUM ('pending', 'completed', 'failed');

-- Step 2: Create Tables
CREATE TABLE users (
    user_id       UUID PRIMARY KEY,
    username      VARCHAR(255) NOT NULL UNIQUE,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE node (
    node_id       UUID PRIMARY KEY,
    username      VARCHAR(255) NOT NULL,
    ram           INT NOT NULL,
    cpu_cores     INT NOT NULL,
    ip_address    VARCHAR(45) NOT NULL,
    port          INT NOT NULL,
    isBusy        BOOLEAN NOT NULL
);

CREATE TABLE node_heartbeat (
    node_id    UUID PRIMARY KEY,
    last_ping  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (node_id) REFERENCES node(node_id) ON DELETE CASCADE
);

CREATE TABLE jobs (
    job_id     UUID PRIMARY KEY,
    user_id    UUID NOT NULL,
    status     job_status DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE data (
    data_id       UUID PRIMARY KEY,
    job_id        UUID NOT NULL,
    size_bytes    BIGINT NOT NULL,
    location_type data_location_type NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- hashed_data 
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE data_location (
    data_id UUID NOT NULL,
    node_id UUID NOT NULL,
    PRIMARY KEY (data_id, node_id),
    FOREIGN KEY (data_id) REFERENCES data(data_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES node(node_id) ON DELETE CASCADE
);

CREATE TABLE tasks (
    task_id       UUID PRIMARY KEY,
    job_id        UUID NOT NULL,
    node_id       UUID DEFAULT NULL,
    data_id       UUID NOT NULL,
    status        task_status DEFAULT 'pending',
    required_ram  INT NOT NULL,
    started_at    TIMESTAMP NULL,
    completed_at  TIMESTAMP NULL,
    retry_count   INT DEFAULT 0,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES node(node_id) ON DELETE SET NULL,
    FOREIGN KEY (data_id) REFERENCES data(data_id) ON DELETE CASCADE
);

CREATE TABLE task_dependencies (
    task_id            UUID NOT NULL,
    depends_on_task_id UUID NOT NULL,
    PRIMARY KEY (task_id, depends_on_task_id),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (depends_on_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE node_logs (
    log_id     SERIAL PRIMARY KEY,
    node_id    UUID NOT NULL,
    task_id    UUID DEFAULT NULL,
    event_type log_event_type NOT NULL,
    timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (node_id) REFERENCES node(node_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE TABLE payments (
    payment_id UUID PRIMARY KEY,
    node_id    UUID NOT NULL,
    amount     DECIMAL(10,2) NOT NULL,
    task_id    UUID DEFAULT NULL,
    status     payment_status DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (node_id) REFERENCES node(node_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE TABLE node_resources (
    node_id UUID PRIMARY KEY,
    rem_ram     INT NOT NULL,
    rem_cpu_cores INT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES node(node_id) ON DELETE CASCADE
);