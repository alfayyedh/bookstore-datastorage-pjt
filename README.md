# DVD Rental - ELT Pipeline Orhcestration
## How to use this project?
1. Requirements
2. Preparations
3. Initializations
4. Orchestrate ELT Pipelines

### 1. Requirements
- OS :
    - Linux
    - WSL (Windows Subsystem For Linux)
- Tools :
    - Dbeaver
    - Docker
    - Cron
    - DBT
- Programming Language :
    - Python
    - SQL
- Python Libray :
    - Luigi
    - Pandas
    - Sentry-SDK
- Platforms :
    - Sentry

### 2. Preparations
- **Clone repo** :
  ```
  # Clone
  git clone https://github.com/rahilaode/dvdrental.git
  ```

- **Create Sentry Project** :
  - Open : https://www.sentry.io
  - Signup with email you want to get notifications abot the error
  - Create Project :
    - Select Platform : Python
    - Set Alert frequency : `On every new issue`
    - Create project name.
  - After create the project, **store SENTRY DSN of your project into .env file**.

- **Create temp dir**. Execute this on root project directory :
    ```
    mkdir pipeline/temp/data
    mkdir pipeline/temp/log
    ```
  
- In thats project directory, **create and use virtual environment**.
- In virtual environment, **install requirements** :
  ```
  pip install -r requirements.txt
  ```

- **Create env file** in project root directory :
  ```
  # Source
  SRC_POSTGRES_DB=...
  SRC_POSTGRES_HOST=...
  SRC_POSTGRES_USER=...
  SRC_POSTGRES_PASSWORD=...
  SRC_POSTGRES_PORT=...

  # DWH
  DWH_POSTGRES_DB=...
  DWH_POSTGRES_HOST=...
  DWH_POSTGRES_USER=...
  DWH_POSTGRES_PASSWORD=...
  DWH_POSTGRES_PORT=...

  # SENTRY DSN
  SENTRY_DSN=... # Fill with your Sentry DSN Project 

  # DIRECTORY
  # Adjust with your directory. make sure to write full path
  DIR_ROOT_PROJECT=...     # <project_dir>
  DIR_TEMP_LOG=...         # <project_dir>/pipeline/temp/log
  DIR_TEMP_DATA=...        # <project_dir>/pipeline/temp/data
  DIR_EXTRACT_QUERY=...    # <project_dir>/pipeline/src_query/extract
  DIR_LOAD_QUERY=...       # <project_dir>/pipeline/src_query/load
  DIR_DBT_TRANSFORM=...  # <project_dir>/pipeline/src_query/dbt_transform
  DIR_LOG=...              # <project_dir>/logs/
    ```

- **Run Data Sources & Data Warehouses** :
  ```
  docker compose up -d
  ```

### 4. Initializations
- Delete existing `dbt_transform` in `./pipeline/src_query/dbt_transform`.
- Init dbt project with name `dbt_transform` in `./pipeline/src_query/`
  ```
  dbt init
  ```
- Test connection to postgres.
  ```
  dbt debug
  ```
- Install dbt packages
  ```
  packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_date
    version: 0.10.0
  ```
- Add `dim_date.csv` into seeds directory.
    - dim_date.csv : https://drive.google.com/file/d/1D1HVjFBHotwC4cWSxBebZTBb62aMQs6d/view?usp=sharing
- Put dim_date file into postgres table.
  ```
  dbt seeds
  ```
- Create staging models. Add this files into staging model
directory.
    - https://drive.google.com/drive/folders/1VRayXPOP0bE8vv9rP9Eduvp60Tn6AmBq?usp=sharing
- Create snapshot. Add this files into snapshots
directory.
    - https://drive.google.com/drive/folders/1MN6DDtDevIMmnHXSi9cGeTFx0rt_hGpw?usp=sharing
### 5. Orchestrate ELT Pipeline
- Create schedule to run pipline every one hour.
  ```
  0 * * * * <project_dir>/elt_run.sh
  ```