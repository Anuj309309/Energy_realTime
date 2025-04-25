import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc
import time
import threading
from flask import Flask, jsonify
from flask_sock import Sock
from flask_cors import CORS
import json
from sqlalchemy import create_engine
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)
CORS(app)
sock = Sock(app)

# SQL Server connection details
server = 'database-2.c5084sk6oq16.ap-south-1.rds.amazonaws.com,1433'
database = 'EnergyDB'
username = 'admin'
password = 'C4i4anuj'

# Create SQLAlchemy engine for Flask server
connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&timeout=30&connect_timeout=30"
engine = create_engine(
    connection_string,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True
)

# Create pyodbc connection for data generation
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
)
cursor = conn.cursor()

# Common parameters for data generation
start_date = datetime(2025, 4, 20)
end_date = datetime(2025, 5, 20)
working_hours = range(9, 18)  # 9 AM to 6 PM
days_of_week = [0, 1, 2, 3, 4, 5]  # Monday to Saturday

# Station configurations
stations = {
    'CoreMaking': {
        'name': 'CoreMaking_Energy',
        'prob_status': [0.8, 0.15, 0.05],
        'power_range': (20, 25),
        'idle_power_range': (2, 4),
        'pf_ranges': {
            'low': (0.70, 0.79, 0.02),
            'high': (0.91, 0.99, 0.12),
            'normal': (0.82, 0.89, 0.86)
        }
    },
    'SandProcessing': {
        'name': 'SandProcessing_Energy',
        'prob_status': [0.8, 0.15, 0.05],
        'power_range': (5, 10),
        'idle_power_range': (0.5, 1),
        'pf_ranges': {
            'low': (0.79, 0.84, 0.07),
            'high': (0.96, 0.99, 0.09),
            'normal': (0.85, 0.95, 0.84)
        }
    },
    'Moulding': {
        'name': 'Moulding_Energy',
        'prob_status': [0.85, 0.10, 0.05],
        'power_range': (30, 35),
        'idle_power_range': (3, 6),
        'pf_ranges': {
            'low': (0.85, 0.89, 0.01),
            'high': (0.99, 1.04, 0.02),
            'normal': (0.90, 0.98, 0.97)
        }
    },
    'Melting': {
        'name': 'Melting_Energy',
        'prob_status': [0.90, 0.05, 0.05],
        'power_range': (350, 400),
        'idle_power_range': (35, 70),
        'pf_ranges': {
            'low': (0.64, 0.69, 0.10),
            'high': (0.96, 0.99, 0.20),
            'normal': (0.70, 0.95, 0.70)
        }
    },
    'Laddle': {
        'name': 'Laddle_Energy',
        'prob_status': [0.85, 0.14, 0.01],
        'power_range': (3, 5),
        'idle_power_range': (0.3, 0.6),
        'pf_ranges': {
            'low': (0.80, 0.84, 0.07),
            'high': (0.90, 1.00, 0.14),
            'normal': (0.85, 0.95, 0.79)
        }
    },
    'PostProcessing': {
        'name': 'PostProcessing_Energy',
        'prob_status': [0.75, 0.20, 0.05],
        'power_range': (90, 100),
        'idle_power_range': (9, 18),
        'pf_ranges': {
            'low': (0.85, 0.89, 0.01),
            'high': (0.99, 1.04, 0.08),
            'normal': (0.90, 0.98, 0.91)
        }
    },
    'Auxiliary': {
        'name': 'AuxiliarySystems_Energy',
        'prob_status': [0.85, 0.10, 0.05],
        'power_range': (3, 5),
        'idle_power_range': (0.3, 0.6),
        'pf_ranges': {
            'low': (0.75, 0.79, 0.05),
            'high': (0.96, 1.00, 0.07),
            'normal': (0.80, 0.95, 0.88)
        }
    }
}

def generate_pf(station_config):
    rand = np.random.rand()
    pf_ranges = station_config['pf_ranges']
    
    if rand < pf_ranges['low'][2]:
        return round(np.random.uniform(pf_ranges['low'][0], pf_ranges['low'][1]), 2)
    elif rand < pf_ranges['low'][2] + pf_ranges['high'][2]:
        return round(np.random.uniform(pf_ranges['high'][0], pf_ranges['high'][1]), 2)
    else:
        return round(np.random.uniform(pf_ranges['normal'][0], pf_ranges['normal'][1]), 2)

def get_db_connection():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def get_next_id(cursor, table_name):
    cursor.execute(f"SELECT MAX(ID) FROM {table_name}")
    max_id = cursor.fetchone()[0]
    return 1 if max_id is None else max_id + 1

def generate_historical_data(station_name, station_config):
    logger.info(f"Generating historical data for {station_name}")
    
    conn = get_db_connection()
    if not conn:
        logger.error(f"Failed to connect to database for {station_name} historical data")
        return
        
    try:
        cursor = conn.cursor()
        current_id = get_next_id(cursor, station_config['name'])
        
        # Generate data from start_date to yesterday
        current_date = start_date
        while current_date.date() < datetime.now().date():
            if current_date.weekday() in days_of_week:
                heat_counter = 1
                for hour in working_hours:
                    heat_no = f"HT_{current_date.strftime('%Y%m%d')}_{heat_counter:03d}"
                    heat_counter += 1
                    
                    for minute in range(60):
                        # Generate data for each minute
                        pf = generate_pf(station_config)
                        power = np.random.uniform(*station_config['power_range'])
                        status = np.random.choice(["Working", "Idle", "Maintenance"], p=station_config['prob_status'])

                        if status == "Idle":
                            power = np.random.uniform(*station_config['idle_power_range'])
                        elif status == "Maintenance":
                            power = 0

                        consumption = power * (1 / 60)  # Convert power (KW) to KVAH for 1 minute
                        reading = consumption  # For historical data, reading equals consumption

                        notification = "Normal PF"
                        if pf < 0.80:
                            notification = "Low PF"
                        elif pf > 0.95:
                            notification = "High PF"

                        # Insert data
                        if station_name == 'Melting':
                            insert_query = f"""
                                INSERT INTO {station_config['name']} (
                                    [ID],
                                    [Station], 
                                    [Date], 
                                    [Time],
                                    [HeatNo],
                                    [Power Factor], 
                                    [Power (KW)], 
                                    [Reading (KVAH)],
                                    [Consumption (KVAH)], 
                                    [Machine Status], 
                                    [Notification]
                                ) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            
                            cursor.execute(insert_query, (
                                current_id,
                                station_config['name'],
                                current_date.date(),
                                current_date.time(),
                                heat_no,
                                pf,
                                round(power, 2),
                                round(reading, 2),
                                round(consumption, 2),
                                status,
                                notification
                            ))
                        else:
                            insert_query = f"""
                                INSERT INTO {station_config['name']} (
                                    [ID],
                                    [Station], 
                                    [Date], 
                                    [Time], 
                                    [Power Factor], 
                                    [Power (KW)], 
                                    [Reading (KVAH)],
                                    [Consumption (KVAH)], 
                                    [Machine Status], 
                                    [Notification]
                                ) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            
                            cursor.execute(insert_query, (
                                current_id,
                                station_config['name'],
                                current_date.date(),
                                current_date.time(),
                                pf,
                                round(power, 2),
                                round(reading, 2),
                                round(consumption, 2),
                                status,
                                notification
                            ))
                        
                        current_id += 1
                        current_date += timedelta(minutes=1)
                        
                # Commit after each hour
                conn.commit()
                logger.debug(f"Committed historical data for {station_name} - {current_date}")
                
            current_date += timedelta(days=1)
            
        cursor.close()
        conn.close()
        logger.info(f"Completed historical data generation for {station_name}")
        
    except Exception as e:
        logger.error(f"Error generating historical data for {station_name}: {str(e)}")
        if conn:
            conn.close()

def generate_historical_melting_data():
    logger.info("Generating historical Melting Production data")
    
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to connect to database for Melting Production historical data")
        return
        
    try:
        cursor = conn.cursor()
        current_id = get_next_id(cursor, 'Melting_Prod')
        
        # Generate data from start_date to yesterday
        current_date = start_date
        while current_date.date() < datetime.now().date():
            if current_date.weekday() in days_of_week:
                heat_counter = 1
                for hour in working_hours:
                    heat_no = f"HT_{current_date.strftime('%Y%m%d')}_{heat_counter:03d}"
                    heat_counter += 1
                    
                    for minute in range(60):
                        # Generate data
                        temperature = np.random.uniform(1000, 1400)
                        
                        # Metal composition based on 15-min intervals
                        if 0 <= minute < 15:
                            composition = {'Fe%': 75, 'C%': 0, 'Cr%': 0, 'Ni%': 0}
                        elif 15 <= minute < 30:
                            composition = {'Fe%': 0, 'C%': 10, 'Cr%': 0, 'Ni%': 0}
                        elif 30 <= minute < 45:
                            composition = {'Fe%': 0, 'C%': 0, 'Cr%': 5, 'Ni%': 0}
                        else:
                            composition = {'Fe%': 0, 'C%': 0, 'Cr%': 0, 'Ni%': 5}
                        
                        # Cumulative Planned and Actual Molten Metal
                        planned = 300 if minute == 0 else 0
                        actual = planned + np.random.uniform(-5, 5) if minute == 0 else 0
                        
                        # Insert data
                        insert_query = """
                            INSERT INTO Melting_Prod (
                                [ID],
                                [Station], 
                                [Date], 
                                [Time],
                                [HeatNo],
                                [Furnace Temperature],
                                [Fe%],
                                [C%],
                                [Cr%],
                                [Ni%],
                                [Cumulative Planned Metal (kg)],
                                [Cumulative Actual Metal (kg)]
                            ) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        cursor.execute(insert_query, (
                            current_id,
                            'Melting_Production',
                            current_date.date(),
                            current_date.time(),
                            heat_no,
                            round(temperature, 2),
                            composition['Fe%'],
                            composition['C%'],
                            composition['Cr%'],
                            composition['Ni%'],
                            round(planned, 2),
                            round(actual, 2)
                        ))
                        
                        current_id += 1
                        current_date += timedelta(minutes=1)
                        
                # Commit after each hour
                conn.commit()
                logger.debug(f"Committed historical data for Melting Production - {current_date}")
                
            current_date += timedelta(days=1)
            
        cursor.close()
        conn.close()
        logger.info("Completed historical data generation for Melting Production")
        
    except Exception as e:
        logger.error(f"Error generating historical Melting Production data: {str(e)}")
        if conn:
            conn.close()

def generate_energy_data(station_name, station_config):
    logger.info(f"Starting real-time data generation for {station_name}")
    
    while True:
        try:
            current_date = datetime.now()
            if current_date.weekday() in days_of_week and current_date.hour in working_hours:
                # Get fresh connection
                conn = get_db_connection()
                if not conn:
                    time.sleep(30)
                    continue
                    
                cursor = conn.cursor()
                current_id = get_next_id(cursor, station_config['name'])
                
                # Generate heat number for Melting
                heat_no = f"HT_{current_date.strftime('%Y%m%d')}_{current_date.hour:03d}"
                
                # Generate data for current minute
                pf = generate_pf(station_config)
                power = np.random.uniform(*station_config['power_range'])
                status = np.random.choice(["Working", "Idle", "Maintenance"], p=station_config['prob_status'])

                if status == "Idle":
                    power = np.random.uniform(*station_config['idle_power_range'])
                elif status == "Maintenance":
                    power = 0

                consumption = power * (1 / 60)  # Convert power (KW) to KVAH for 1 minute
                reading = consumption  # For real-time data, reading equals consumption

                notification = "Normal PF"
                if pf < 0.80:
                    notification = "Low PF"
                elif pf > 0.95:
                    notification = "High PF"

                # Insert data into database
                if station_name == 'Melting':
                    insert_query = f"""
                        INSERT INTO {station_config['name']} (
                            [ID],
                            [Station], 
                            [Date], 
                            [Time],
                            [HeatNo],
                            [Power Factor], 
                            [Power (KW)], 
                            [Reading (KVAH)],
                            [Consumption (KVAH)], 
                            [Machine Status], 
                            [Notification]
                        ) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    cursor.execute(insert_query, (
                        current_id,
                        station_config['name'],
                        current_date.date(),
                        current_date.time(),
                        heat_no,
                        pf,
                        round(power, 2),
                        round(reading, 2),
                        round(consumption, 2),
                        status,
                        notification
                    ))
                else:
                    insert_query = f"""
                        INSERT INTO {station_config['name']} (
                            [ID],
                            [Station], 
                            [Date], 
                            [Time], 
                            [Power Factor], 
                            [Power (KW)], 
                            [Reading (KVAH)],
                            [Consumption (KVAH)], 
                            [Machine Status], 
                            [Notification]
                        ) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    cursor.execute(insert_query, (
                        current_id,
                        station_config['name'],
                        current_date.date(),
                        current_date.time(),
                        pf,
                        round(power, 2),
                        round(reading, 2),
                        round(consumption, 2),
                        status,
                        notification
                    ))
                conn.commit()
                
                logger.debug(f"Inserted real-time data for {station_name}")
                
                # Close connection
                cursor.close()
                conn.close()
                
                # Wait for 1 minute before next insert
                time.sleep(60)
            
            # Sleep for 30 seconds before checking again
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Error in {station_name} real-time data generation: {str(e)}")
            time.sleep(30)  # Wait before retrying

def generate_melting_production_data():
    logger.info("Starting real-time Melting Production data generation")
    
    while True:
        try:
            current_date = datetime.now()
            if current_date.weekday() in days_of_week and current_date.hour in working_hours:
                # Get fresh connection
                conn = get_db_connection()
                if not conn:
                    time.sleep(30)
                    continue
                    
                cursor = conn.cursor()
                current_id = get_next_id(cursor, 'Melting_Prod')
                
                # Generate heat number
                heat_no = f"HT_{current_date.strftime('%Y%m%d')}_{current_date.hour:03d}"
                
                # Generate data
                temperature = np.random.uniform(1000, 1400)
                
                # Metal composition based on 15-min intervals
                minute = current_date.minute
                if 0 <= minute < 15:
                    composition = {'Fe%': 75, 'C%': 0, 'Cr%': 0, 'Ni%': 0}
                elif 15 <= minute < 30:
                    composition = {'Fe%': 0, 'C%': 10, 'Cr%': 0, 'Ni%': 0}
                elif 30 <= minute < 45:
                    composition = {'Fe%': 0, 'C%': 0, 'Cr%': 5, 'Ni%': 0}
                else:
                    composition = {'Fe%': 0, 'C%': 0, 'Cr%': 0, 'Ni%': 5}
                
                # Cumulative Planned and Actual Molten Metal
                planned = 300 if minute == 0 else 0
                actual = planned + np.random.uniform(-5, 5) if minute == 0 else 0
                
                # Insert data into database
                insert_query = """
                    INSERT INTO Melting_Prod (
                        [ID],
                        [Station], 
                        [Date], 
                        [Time],
                        [HeatNo],
                        [Furnace Temperature],
                        [Fe%],
                        [C%],
                        [Cr%],
                        [Ni%],
                        [Cumulative Planned Metal (kg)],
                        [Cumulative Actual Metal (kg)]
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_query, (
                    current_id,
                    'Melting_Production',
                    current_date.date(),
                    current_date.time(),
                    heat_no,
                    round(temperature, 2),
                    composition['Fe%'],
                    composition['C%'],
                    composition['Cr%'],
                    composition['Ni%'],
                    round(planned, 2),
                    round(actual, 2)
                ))
                conn.commit()
                
                logger.debug("Inserted real-time Melting Production data")
                
                # Close connection
                cursor.close()
                conn.close()
                
                # Wait for 1 minute before next insert
                time.sleep(60)
            
            # Sleep for 30 seconds before checking again
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Error in Melting Production real-time data generation: {str(e)}")
            time.sleep(30)  # Wait before retrying

def clear_all_tables():
    logger.info("Checking and clearing all tables...")
    tables = [
        'CoreMaking_Energy',
        'SandProcessing_Energy',
        'Moulding_Energy',
        'Melting_Energy',
        'Laddle_Energy',
        'PostProcessing_Energy',
        'AuxiliarySystems_Energy',
        'Melting_Prod'
    ]
    
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to connect to database for table clearing")
        return False
        
    try:
        cursor = conn.cursor()
        for table in tables:
            # Check if table has data
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            if row_count > 0:
                logger.info(f"Found {row_count} rows in {table}, deleting...")
                cursor.execute(f"DELETE FROM {table}")
                conn.commit()
                logger.info(f"Cleared {table} table")
            else:
                logger.info(f"{table} table is empty")
                
        cursor.close()
        conn.close()
        logger.info("All tables checked and cleared successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error clearing tables: {str(e)}")
        if conn:
            conn.close()
        return False

# Flask routes
@app.route('/')
def health_check():
    return jsonify({"status": "ok", "message": "Energy Monitoring API is running"})

@sock.route('/ws')
def handle_websocket(ws):
    logger.info('New WebSocket connection established')
    try:
        # Send initial connection success message
        ws.send(json.dumps({
            'event': 'connection_established',
            'data': {'message': 'WebSocket connection established successfully'}
        }))
        
        while True:
            # Get latest energy data
            logger.debug('Fetching latest energy data...')
            query = "SELECT Process, Power, Consumption, PowerFactor FROM Latest_AllEnergy_Readings_View"
            df = pd.read_sql(query, engine)
            data = {
                'event': 'latest_energy_data',
                'data': df.to_dict(orient='records')
            }
            ws.send(json.dumps(data))
            logger.debug('Sent latest energy data')

            # Get current power
            logger.debug('Fetching current power...')
            query = "SELECT Round(SUM(Power),1) AS TotalPower FROM Latest_Energy_Reading_View"
            df = pd.read_sql(query, engine)
            current_power = float(df["TotalPower"].iloc[0]) if not df.empty and df["TotalPower"].notna().iloc[0] else 0
            data = {
                'event': 'current_power',
                'data': {'TotalPower': current_power}
            }
            ws.send(json.dumps(data))
            logger.debug('Sent current power data')

            # Get today's data
            query = "SELECT Top 1 Round([Total_Consumption],1) AS TodayConsumption FROM [EnergyDB].[dbo].[Daily_Consumption_View] ORDER BY Date Desc;"
            df = pd.read_sql(query, engine)
            today_consumption = float(df["TodayConsumption"].iloc[0]) if not df.empty and df["TodayConsumption"].notna().iloc[0] else 0
            
            query = "SELECT TOP 1 Round([Daily_Production],1) AS TodayProduction FROM [EnergyDB].[dbo].[Daily_Production_View] ORDER BY Date Desc;"
            df = pd.read_sql(query, engine)
            today_production = float(df["TodayProduction"].iloc[0]) if not df.empty and df["TodayProduction"].notna().iloc[0] else 0
            
            data = {
                'event': 'today_data',
                'data': {
                    'TodayConsumption': today_consumption,
                    'TodayProduction': today_production
                }
            }
            ws.send(json.dumps(data))
            logger.debug('Sent today\'s data')

            # Get power view data
            query = """
            SELECT Date, Time, SUM(Power) AS Total_Power_KW
            FROM Last9_Energy_Readings_Vw
            GROUP BY Date, Time
            ORDER BY Date ASC, Time ASC;
            """
            df = pd.read_sql(query, engine)
            df['Time'] = df['Time'].astype(str)
            df['Date'] = df['Date'].astype(str)
            df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time']).astype(str)
            data = {
                'event': 'power_view',
                'data': df.to_dict(orient='records')
            }
            ws.send(json.dumps(data))
            logger.debug('Sent power view data')

            # Get monthly data
            query = "SELECT Round(SUM([Total_Consumption]),1) AS ThisMonthConsumption FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE());"
            df = pd.read_sql(query, engine)
            this_month_consumption = float(df["ThisMonthConsumption"].iloc[0]) if not df.empty and df["ThisMonthConsumption"].notna().iloc[0] else 0
            
            query = "SELECT Round(SUM([Total_Consumption]),1) AS PreviousMonthConsumption FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(DATEADD(MONTH, -1, GETDATE())) AND MONTH(Date) = MONTH(DATEADD(MONTH, -1, GETDATE()));"
            df = pd.read_sql(query, engine)
            prev_month_consumption = float(df["PreviousMonthConsumption"].iloc[0]) if not df.empty and df["PreviousMonthConsumption"].notna().iloc[0] else 0
            
            data = {
                'event': 'monthly_data',
                'data': {
                    'ThisMonthConsumption': this_month_consumption,
                    'PreviousMonthConsumption': prev_month_consumption
                }
            }
            ws.send(json.dumps(data))
            logger.debug('Sent monthly data')

            # Get consumption per tonne data
            query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())) / (SELECT SUM([Daily_Production])/1000 FROM [EnergyDB].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())),1) AS ThisMonthConsumptionPerTonne;"
            df = pd.read_sql(query, engine)
            this_month_per_tonne = float(df["ThisMonthConsumptionPerTonne"].iloc[0]) if not df.empty and df["ThisMonthConsumptionPerTonne"].notna().iloc[0] else 0
            
            query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1) / (SELECT SUM([Daily_Production])/1000 FROM [EnergyDB].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1),1) AS PreviousMonthConsumptionPerTonne;"
            df = pd.read_sql(query, engine)
            prev_month_per_tonne = float(df["PreviousMonthConsumptionPerTonne"].iloc[0]) if not df.empty and df["PreviousMonthConsumptionPerTonne"].notna().iloc[0] else 0
            
            data = {
                'event': 'consumption_per_tonne',
                'data': {
                    'ThisMonthConsumptionPerTonne': this_month_per_tonne,
                    'PreviousMonthConsumptionPerTonne': prev_month_per_tonne
                }
            }
            ws.send(json.dumps(data))
            logger.debug('Sent consumption per tonne data')

            # Sleep for 5 seconds before next update
            time.sleep(5)
            
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        # Send error message to client
        try:
            error_data = {
                'event': 'error',
                'data': {'message': str(e)}
            }
            ws.send(json.dumps(error_data))
        except:
            pass
    finally:
        logger.info('WebSocket connection closed')

def start_data_generation():
    # First clear all tables
    if not clear_all_tables():
        logger.error("Failed to clear tables, aborting data generation")
        return []
    
    # Generate historical data first
    logger.info("Starting historical data generation...")
    historical_threads = []
    
    # Start historical data generation for each station
    for station_name, station_config in stations.items():
        thread = threading.Thread(target=generate_historical_data, args=(station_name, station_config))
        thread.daemon = True
        thread.start()
        historical_threads.append(thread)
    
    # Start historical melting production data generation
    melting_thread = threading.Thread(target=generate_historical_melting_data)
    melting_thread.daemon = True
    melting_thread.start()
    historical_threads.append(melting_thread)
    
    # Wait for historical data generation to complete
    for thread in historical_threads:
        thread.join()
    
    logger.info("Historical data generation completed, starting real-time data generation...")
    
    # Start real-time data generation threads
    realtime_threads = []
    for station_name, station_config in stations.items():
        thread = threading.Thread(target=generate_energy_data, args=(station_name, station_config))
        thread.daemon = True
        thread.start()
        realtime_threads.append(thread)
    
    # Start real-time melting production data generation
    melting_thread = threading.Thread(target=generate_melting_production_data)
    melting_thread.daemon = True
    melting_thread.start()
    realtime_threads.append(melting_thread)
    
    return realtime_threads

if __name__ == '__main__':
    # Start data generation threads
    data_threads = start_data_generation()
    
    # Production configuration
    port = int(os.environ.get('PORT', 5000))
    host = '0.0.0.0'
    
    # Disable debug mode in production
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Server starting on {host}:{port}")
    logger.info(f"WebSocket endpoint available at ws://{host}:{port}/ws")
    
    # Run the Flask application
    app.run(host=host, port=port, debug=debug)
