import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from pipeline.extract import Extract
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load(luigi.Task):
    
    def requires(self):
        return Extract()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            # Read query to truncate staging schema in dwh
            truncate_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/staging-truncate_tables.sql'
            )
            
            
            logging.info("Read Load Query - SUCCESS")
            
        except Exception:
            logging.error("Read Load Query - FAILED")
            raise Exception("Failed to read Load Query")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read Data to be load
        try:
            # Read csv
            address = pd.read_csv(self.input()[0].path)
            address_status = pd.read_csv(self.input()[1].path)
            author = pd.read_csv(self.input()[2].path)
            book = pd.read_csv(self.input()[3].path)
            book_author = pd.read_csv(self.input()[4].path)
            book_language = pd.read_csv(self.input()[5].path)
            address_status = pd.read_csv(self.input()[6].path)
            cust_order = pd.read_csv(self.input()[7].path)
            customer = pd.read_csv(self.input()[8].path)
            customer_address = pd.read_csv(self.input()[9].path)
            order_history = pd.read_csv(self.input()[10].path)
            order_line = pd.read_csv(self.input()[11].path)
            order_status = pd.read_csv(self.input()[12].path)
            publisher = pd.read_csv(self.input()[13].path)
            shipping_method = pd.read_csv(self.input()[14].path)
            
            logging.info(f"Read Extracted Data - SUCCESS")
            
        except Exception:
            logging.error(f"Read Extracted Data  - FAILED")
            raise Exception("Failed to Read Extracted Data")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Truncate all tables before load
        # This puropose to avoid errors because duplicate key value violates unique constraint
        try:            
            # Split the SQL queries if multiple queries are present
            truncate_query = truncate_query.split(';')

            # Remove newline characters and leading/trailing whitespaces
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()

            # Execute each query
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
                
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Truncate staging Schema in DWH - SUCCESS")
        
        except Exception:
            logging.error(f"Truncate staging Schema in DWH - FAILED")
            
            raise Exception("Failed to Truncate staging Schema in DWH")
        
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for loading tables
        start_time = time.time()  
        logging.info("==================================STARTING LOAD DATA=======================================")
        # Load to tables to staging schema
        try:
            
            try:
                # Load actor tables    
                actor.to_sql('address', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.address' - SUCCESS")
                
                # Load address_status tables
                address_status.to_sql('address_status', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'staging')
                logging.info(f"LOAD 'staging.address_status' - SUCCESS")
                
                # Load author tables
                author.to_sql('author', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'staging')
                logging.info(f"LOAD 'staging.author' - SUCCESS")
                
                
                # Load book tables
                book.to_sql('book', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.book' - SUCCESS")
                
                
                # Load book_author tables
                book_author.to_sql('book_author', 
                                con = dwh_engine, 
                                if_exists = 'append', 
                                index = False, 
                                schema = 'staging')
                logging.info(f"LOAD 'staging.book_author' - SUCCESS")                
                
                # Load book_language tables
                book_language.to_sql('book_language', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'staging')
                logging.info(f"LOAD 'staging.book_language' - SUCCESS")
                
                # Load country tables
                country.to_sql('country', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.country' - SUCCESS")
                
                
                # Load cust_order tables
                cust_order.to_sql('cust_order', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.cust_order' - SUCCESS")                
                
                # Load customer tables
                customer.to_sql('customer', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.customer' - SUCCESS")
                
                # Load customer_address tables
                customer_address.to_sql('customer_address', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.customer_address' - SUCCESS")
                
                # Load order_history tables
                order_history.to_sql('order_history', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.order_history' - SUCCESS")
                
                # Load order_line tables
                order_line.to_sql('order_line', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.order_line' - SUCCESS")
                
                # Load order_status tables
                order_status.to_sql('order_status', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.order_status' - SUCCESS")
                
                # Load publisher tables
                publisher.to_sql('publisher', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.publisher' - SUCCESS")
                
                # Load shipping_method tables
                shipping_method.to_sql('shipping_method', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'staging')
                logging.info(f"LOAD 'staging.shipping_method' - SUCCESS")
                
                logging.info(f"LOAD 'staging.store' - SUCCESS")
                logging.info(f"LOAD All Tables To DWH-staging - SUCCESS")
                
            except Exception:
                logging.error(f"LOAD All Tables To DWH-staging - FAILED")
                raise Exception('Failed Load Tables To DWH-staging')        
        
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
                        
        #----------------------------------------------------------------------------------------------------------------------------------------
        except Exception:
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            logging.error("LOAD All Tables To DWH - FAILED")
            raise Exception('Failed Load Tables To DWH')   
        
        logging.info("==================================ENDING LOAD DATA=======================================")
        
    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]
        