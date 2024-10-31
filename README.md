# ETL pipeline from Weather API to PostgreSQL using Apache Airflow

## Table of Contents

- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)
- [Contact](#contact)

## Description

- This is a project to **complete it**

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/jamwalshah/airflow-etl-weather-api-to-postgres.git
    ```

2. Navigate to the project directory:

    ```bash
    cd airflow-etl-weather-api-to-postgres
    ```

3. Create the virtual environment and install the dependencies:

    - For windows:

        ```bash
        python -m venv env
        env\Scripts\activate
        pip install -r requirements.txt
        ```

    - For Linux/Mac:

        ```bash
        python3 -m venv env
        env/bin/activate
        pip3 install -r requirements.txt
        ```

4. Install Docker Desktop from [Link](https://docs.docker.com/engine/install/)
5. Install Astro CLI from [Link](https://www.astronomer.io/docs/astro/cli/install-cli).This will help us to start Airflow locally to orchestrate data workflow.

## Usage

1. Setup the `.env` file for system variables
2. Run the project with Astro using command below, it will build your project and create 4 Docker containers for each of Airflow components required to run a DAG including  the Scheduler, Web-server, Triggerer and a PostgreSQL database.

    ```bash
    astro dev start
    ```

3. After project build successfully, Open Airflow Web UI in web-browser at [https://localhost:8080/](https://localhost:8080/)
4. Login using `admin` as default username and `admin` as default password
5. Under `DAGs` tab, you'll see all the DAG weather ETL **Complete it**

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for the details.

## Contact

[Linkedin/jamwalshah](https://linkedin.com/in/jamwalshah/)
