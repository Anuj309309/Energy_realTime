from flask import Flask, jsonify
from flask_sock import Sock
import pandas as pd
from datetime import datetime
import time
import threading
import json
from sqlalchemy import create_engine

app = Flask(__name__)
sock = Sock(app)

# SQL Server Connection Details
server = 'ICPL-24-25-LAPT'
database = 'Energy Monitoring_Realtime'

# Create SQLAlchemy engine
connection_string = f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
engine = create_engine(connection_string)

@sock.route('/ws')
def handle_websocket(ws):
    print('Client connected')
    try:
        while True:
            # Get latest energy data
            query = "SELECT Process, Power, Consumption, PowerFactor FROM Latest_AllEnergy_Readings_View"
            df = pd.read_sql(query, engine)
            data = {
                'event': 'latest_energy_data',
                'data': df.to_dict(orient='records')
            }
            ws.send(json.dumps(data))

            # Get current power
            query = "SELECT Round(SUM(Power),1) AS TotalPower FROM Latest_Energy_Reading_View"
            df = pd.read_sql(query, engine)
            current_power = float(df["TotalPower"].iloc[0]) if not df.empty and df["TotalPower"].notna().iloc[0] else 0
            data = {
                'event': 'current_power',
                'data': {'TotalPower': current_power}
            }
            ws.send(json.dumps(data))

            # Get today's data
            query = "SELECT Top 1 Round([Total_Consumption],1) AS TodayConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] ORDER BY Date Desc;"
            df = pd.read_sql(query, engine)
            today_consumption = float(df["TodayConsumption"].iloc[0]) if not df.empty and df["TodayConsumption"].notna().iloc[0] else 0
            
            query = "SELECT TOP 1 Round([Daily_Production],1) AS TodayProduction FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] ORDER BY Date Desc;"
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

            # Get monthly data
            query = "SELECT Round(SUM([Total_Consumption]),1) AS ThisMonthConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE());"
            df = pd.read_sql(query, engine)
            this_month_consumption = float(df["ThisMonthConsumption"].iloc[0]) if not df.empty and df["ThisMonthConsumption"].notna().iloc[0] else 0
            
            query = "SELECT Round(SUM([Total_Consumption]),1) AS PreviousMonthConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(DATEADD(MONTH, -1, GETDATE())) AND MONTH(Date) = MONTH(DATEADD(MONTH, -1, GETDATE()));"
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

            # Get consumption per tonne data
            query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())) / (SELECT SUM([Daily_Production])/1000 FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())),1) AS ThisMonthConsumptionPerTonne;"
            df = pd.read_sql(query, engine)
            this_month_per_tonne = float(df["ThisMonthConsumptionPerTonne"].iloc[0]) if not df.empty and df["ThisMonthConsumptionPerTonne"].notna().iloc[0] else 0
            
            query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1) / (SELECT SUM([Daily_Production])/1000 FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1),1) AS PreviousMonthConsumptionPerTonne;"
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

            # Sleep for 15 seconds before next update
            time.sleep(15)
    except Exception as e:
        print(f"WebSocket error: {str(e)}")
    finally:
        print('Client disconnected')

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
    query = "SELECT Top 1 Round([Total_Consumption],1) AS TodayConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] ORDER BY Date Desc;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["TodayConsumption"].notna().iloc[0]:
        return jsonify({"TodayConsumption": float(df["TodayConsumption"].iloc[0])})
    return jsonify({"TodayConsumption": 0})

@app.route('/api/today_production', methods=['GET'])
def get_today_production():
    query = "SELECT TOP 1 Round([Daily_Production],1) AS TodayProduction FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] ORDER BY Date Desc;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["TodayProduction"].notna().iloc[0]:
        return jsonify({"TodayProduction": float(df["TodayProduction"].iloc[0])})
    return jsonify({"TodayProduction": 0})

@app.route('/api/this_month_consumption', methods=['GET'])
def get_this_month_consumption():
    query = "SELECT Round(SUM([Total_Consumption]),1) AS ThisMonthConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE());"
    df = pd.read_sql(query, engine)
    if not df.empty and df["ThisMonthConsumption"].notna().iloc[0]:
        return jsonify({"ThisMonthConsumption": float(df["ThisMonthConsumption"].iloc[0])})
    return jsonify({"ThisMonthConsumption": 0})

@app.route('/api/previous_month_consumption', methods=['GET'])
def get_previous_month_consumption():
    query = "SELECT Round(SUM([Total_Consumption]),1) AS PreviousMonthConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(DATEADD(MONTH, -1, GETDATE())) AND MONTH(Date) = MONTH(DATEADD(MONTH, -1, GETDATE()));"
    df = pd.read_sql(query, engine)
    if not df.empty and df["PreviousMonthConsumption"].notna().iloc[0]:
        return jsonify({"PreviousMonthConsumption": float(df["PreviousMonthConsumption"].iloc[0])})
    return jsonify({"PreviousMonthConsumption": 0})

@app.route('/api/this_month_consumption_per_tonne', methods=['GET'])
def get_this_month_consumption_per_tonne():
    query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())) / (SELECT SUM([Daily_Production])/1000 FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())),1) AS ThisMonthConsumptionPerTonne;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["ThisMonthConsumptionPerTonne"].notna().iloc[0]:
        return jsonify({"ThisMonthConsumptionPerTonne": float(df["ThisMonthConsumptionPerTonne"].iloc[0])})
    return jsonify({"ThisMonthConsumptionPerTonne": 0})

@app.route('/api/previous_month_consumption_per_tonne', methods=['GET'])
def get_previous_month_consumption_per_tonne():
    query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1) / (SELECT SUM([Daily_Production])/1000 FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1),1) AS PreviousMonthConsumptionPerTonne;"
    df = pd.read_sql(query, engine)
    if not df.empty and df["PreviousMonthConsumptionPerTonne"].notna().iloc[0]:
        return jsonify({"PreviousMonthConsumptionPerTonne": float(df["PreviousMonthConsumptionPerTonne"].iloc[0])})
    return jsonify({"PreviousMonthConsumptionPerTonne": 0})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 