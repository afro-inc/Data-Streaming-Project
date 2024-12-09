# Data Streaming Showcase with Airflow, Kafka, PySpark, and MySQL

This project demonstrates the integration of Apache Airflow, Apache Kafka, and PySpark for a seamless data streaming pipeline, using MySQL as the final sink for processed data. The infrastructure is containerized with Docker for easy deployment and management.

## Project Components

The project comprises the following main components:

1. **Docker**: Used to containerize all tools for consistent and portable deployment.
2. **Apache Airflow**: Handles job scheduling and executes Python scripts, sending results to Kafka.
3. **Apache Kafka**: Acts as the message broker for streaming data between the source and sink.
4. **PySpark**: Processes and transforms the data, then writes the results to the destination database.
5. **MySQL**: Serves as the final destination for storing processed data.

## Setup and Usage

### 1. Starting the Environment
To set up and start all required containers, use the provided PowerShell script:

```powershell
Start-Job -FilePath .\run.ps1
```

This script will initialize and configure Docker containers for all components.

### 2. Initiating the Streaming Process
- Access the Airflow webserver by navigating to [http://localhost:8080/](http://localhost:8080/) in your web browser.
- Log in with the following credentials:
  - **Username**: `airflow`
  - **Password**: `airflow`
- Once logged in, start the DAG (Directed Acyclic Graph) for the streaming process. If no tasks are currently running, initiate the DAG manually.

### 3. Viewing and Verifying Data
The PySpark instance is automatically launched by the PowerShell script. Once the pipeline is active, you can view the resulting data in the MySQL database:

- Connect to the MySQL instance using your preferred database tool (e.g., MySQL Workbench).
- Connection details:
  - **Host**: `localhost`
  - **Port**: `3306`
  - **Username**: `root`
  - **Password**: `admin`

### 4. Additional Notes
- Ensure Docker is installed and running on your system before executing the PowerShell script.
- The pipeline uses a local setup and may need adjustments for production or distributed environments.

## Acknowledgements

Thank you for exploring this project. I hope it serves as a helpful example of integrating modern data streaming tools for practical use cases. Feel free to contribute or reach out with any questions!
