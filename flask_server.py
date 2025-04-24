from flask import Flask, jsonify
from flask_sock import Sock
from flask_cors import CORS
import pandas as pd
from datetime import datetime
import time
import threading
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

app = Flask(__name__)
# Enable CORS for all routes
CORS(app)
sock = Sock(app)

# Add root route for health checks
@app.route('/')
def health_check():
    return jsonify({"status": "ok", "message": "Energy Monitoring API is running"})

# Add WebSocket connection test endpoint
@app.route('/ws-test')
def ws_test():
    return jsonify({
        "status": "ok",
        "message": "WebSocket endpoint is available",
        "ws_url": f"ws://{request.host}/ws"
    })

# SQL Server connection details
server = 'database-2.c5084sk6oq16.ap-south-1.rds.amazonaws.com,1433'
database = 'EnergyDB'
username = 'admin'
password = 'C4i4anuj'

# Create SQLAlchemy engine with proper connection pooling and timeout settings
connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&timeout=30&connect_timeout=30"
engine = create_engine(
    connection_string,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True
)

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
            try:
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
                logger.debug('Fetching today\'s data...')
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
                logger.error(f"Error in WebSocket loop: {str(e)}")
                # Send error message to client
                error_data = {
                    'event': 'error',
                    'data': {'message': str(e)}
                }
                ws.send(json.dumps(error_data))
                # Wait for 5 seconds before retrying
                time.sleep(5)
                
    except Exception as e:
        logger.error(f"WebSocket connection error: {str(e)}")
    finally:
        logger.info('WebSocket connection closed')

# Keep the existing REST endpoints for backward compatibility
@app.route('/api/latest_energy_data', methods=['GET'])
def get_latest_energy_data():
    query = "SELECT Process, Power, Consumption, PowerFactor FROM Latest_AllEnergy_Readings_View"
    df = pd.read_sql(query, engine)
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/power_view', methods=['GET'])
def get_power_view():
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
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/daily_consumption', methods=['GET'])
def get_daily_consumption():
    query = "SELECT Date, Total_Consumption FROM Daily_Consumption_View ORDER BY Date ASC"
    df = pd.read_sql(query, engine)
    df['Date'] = df['Date'].astype(str)
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/daily_production', methods=['GET'])
def get_daily_production():
    query = "SELECT Date, Daily_Production FROM Daily_Production_View ORDER BY Date ASC"
    df = pd.read_sql(query, engine)
    df['Date'] = df['Date'].astype(str)
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/current_power', methods=['GET'])
def get_current_power():
    query = "SELECT Round(SUM(Power),1) AS TotalPower FROM Latest_Energy_Reading_View"
    df = pd.read_sql(query, engine)
    if not df.empty and df["TotalPower"].notna().iloc[0]:
        return jsonify({"TotalPower": float(df["TotalPower"].iloc[0])})
    return jsonify({"TotalPower": 0})

@app.route('/api/today_consumption', methods=['GET'])
def get_today_consumption():
    query = "SELECT Top 1 Round([Total_Consumption],1) AS TodayConsumption FROM [EnergyDB].[dbo].[Daily_Consumption_View] ORDER BY Date Desc;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["TodayConsumption"].notna().iloc[0]:
        return jsonify({"TodayConsumption": float(df["TodayConsumption"].iloc[0])})
    return jsonify({"TodayConsumption": 0})

@app.route('/api/today_production', methods=['GET'])
def get_today_production():
    query = "SELECT TOP 1 Round([Daily_Production],1) AS TodayProduction FROM [EnergyDB].[dbo].[Daily_Production_View] ORDER BY Date Desc;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["TodayProduction"].notna().iloc[0]:
        return jsonify({"TodayProduction": float(df["TodayProduction"].iloc[0])})
    return jsonify({"TodayProduction": 0})

@app.route('/api/this_month_consumption', methods=['GET'])
def get_this_month_consumption():
    query = "SELECT Round(SUM([Total_Consumption]),1) AS ThisMonthConsumption FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE());"
    df = pd.read_sql(query, engine)
    if not df.empty and df["ThisMonthConsumption"].notna().iloc[0]:
        return jsonify({"ThisMonthConsumption": float(df["ThisMonthConsumption"].iloc[0])})
    return jsonify({"ThisMonthConsumption": 0})

@app.route('/api/previous_month_consumption', methods=['GET'])
def get_previous_month_consumption():
    query = "SELECT Round(SUM([Total_Consumption]),1) AS PreviousMonthConsumption FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(DATEADD(MONTH, -1, GETDATE())) AND MONTH(Date) = MONTH(DATEADD(MONTH, -1, GETDATE()));"
    df = pd.read_sql(query, engine)
    if not df.empty and df["PreviousMonthConsumption"].notna().iloc[0]:
        return jsonify({"PreviousMonthConsumption": float(df["PreviousMonthConsumption"].iloc[0])})
    return jsonify({"PreviousMonthConsumption": 0})

@app.route('/api/this_month_consumption_per_tonne', methods=['GET'])
def get_this_month_consumption_per_tonne():
    query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())) / (SELECT SUM([Daily_Production])/1000 FROM [EnergyDB].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())),1) AS ThisMonthConsumptionPerTonne;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["ThisMonthConsumptionPerTonne"].notna().iloc[0]:
        return jsonify({"ThisMonthConsumptionPerTonne": float(df["ThisMonthConsumptionPerTonne"].iloc[0])})
    return jsonify({"ThisMonthConsumptionPerTonne": 0})

@app.route('/api/previous_month_consumption_per_tonne', methods=['GET'])
def get_previous_month_consumption_per_tonne():
    query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [EnergyDB].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1) / (SELECT SUM([Daily_Production])/1000 FROM [EnergyDB].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1),1) AS PreviousMonthConsumptionPerTonne;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["PreviousMonthConsumptionPerTonne"].notna().iloc[0]:
        return jsonify({"PreviousMonthConsumptionPerTonne": float(df["PreviousMonthConsumptionPerTonne"].iloc[0])})
    return jsonify({"PreviousMonthConsumptionPerTonne": 0})

if __name__ == '__main__':
    # Production configuration
    port = int(os.environ.get('PORT', 5000))
    host = '0.0.0.0'
    
    # Disable debug mode in production
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Server starting on {host}:{port}")
    logger.info(f"WebSocket endpoint available at ws://{host}:{port}/ws")
    
    # Run the application
    app.run(host=host, port=port, debug=debug) 
