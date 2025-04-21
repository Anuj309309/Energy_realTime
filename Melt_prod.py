import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from datetime import datetime
import time

# Parameters
start_date = datetime(2025, 3, 1)
end_date = datetime(2025, 5, 20)
working_hours = range(9, 18)  # 9 AM to 6 PM

days_of_week = [0, 1, 2, 3, 4, 5]  # Monday to Saturday
planned_metal_per_hour = 300  # kg
metal_composition = {'Fe%': 75, 'C%': 10, 'Cr%': 5, 'Ni%': 5}


# Data generation
timestamps = []
heat_numbers = []
temperatures = []
cumulative_planned = []
cumulative_actual = []
composition_data = {'Fe%': [], 'C%': [], 'Cr%': [], 'Ni%': []}

current_planned_total = 0
current_actual_total = 0
current_day = None


# Generate data
current_date = start_date
heat_counter = 1

while current_date <= end_date:
    if current_date.weekday() in days_of_week:
        current_day = current_date.date()
        current_planned_total = 0
        current_actual_total = 0
        heat_counter = 1  # Reset heat counter daily
        
        for hour in working_hours:
            heat_no = f"HT_{current_date.strftime('%Y%m%d')}_{heat_counter:03d}"
            heat_counter += 1
            
            for minute in range(60):  # 1-minute intervals
                timestamp = current_date.replace(hour=hour, minute=minute, second=0)
                timestamps.append(timestamp)
                heat_numbers.append(heat_no)
                
                # Simulated furnace temperature (1000-1400°C)
                temperature = np.random.uniform(1000, 1400)
                temperatures.append(round(temperature, 2))
                
                # Add metal composition based on 15-min intervals
                if 0 <= minute < 15:
                    composition_data['Fe%'].append(75)
                    composition_data['C%'].append(0)
                    composition_data['Cr%'].append(0)
                    composition_data['Ni%'].append(0)
                elif 15 <= minute < 30:
                    composition_data['Fe%'].append(0)
                    composition_data['C%'].append(10)
                    composition_data['Cr%'].append(0)
                    composition_data['Ni%'].append(0)
                elif 30 <= minute < 45:
                    composition_data['Fe%'].append(0)
                    composition_data['C%'].append(0)
                    composition_data['Cr%'].append(5)
                    composition_data['Ni%'].append(0)
                else:
                    composition_data['Fe%'].append(0)
                    composition_data['C%'].append(0)
                    composition_data['Cr%'].append(0)
                    composition_data['Ni%'].append(5)
                
                # Cumulative Planned and Actual Molten Metal
                planned = planned_metal_per_hour if minute == 0 else 0
                actual = planned + np.random.uniform(-5, 5) if minute == 0 else 0
                
                current_planned_total += planned
                current_actual_total += actual
                
                cumulative_planned.append(round(current_planned_total, 2))
                cumulative_actual.append(round(current_actual_total, 2))
        
    current_date += timedelta(days=1)


# Create DataFrame
df = pd.DataFrame({
    'ID': range(1, len(timestamps) + 1),
    'Timestamp': timestamps,
    'HeatNo': heat_numbers,
    'Furnace Temperature': temperatures,
    'Fe%': composition_data['Fe%'],
    'C%': composition_data['C%'],
    'Cr%': composition_data['Cr%'],
    'Ni%': composition_data['Ni%'],
    'Cumulative Planned Metal (kg)': cumulative_planned,
    'Cumulative Actual Metal (kg)': cumulative_actual
})



# Add Station column
df['Station'] = 'Melting_Production'

# Split Timestamp into separate Date and Time columns
df['Date'] = df['Timestamp'].dt.date
df['Time'] = df['Timestamp'].dt.time

# Drop the original Timestamp column (if not required)
df.drop(columns=['Timestamp'], inplace=True)

# Reorder columns for better readability
df = df[['ID', 'Station', 'Date', 'Time', 'HeatNo', 'Furnace Temperature', 'Fe%', 'C%', 'Cr%', 'Ni%',
         'Cumulative Planned Metal (kg)', 'Cumulative Actual Metal (kg)']]



# SQL Server connection details
server = 'ICPL-24-25-LAPT'
database = 'Energy Monitoring_Realtime'


# Create the connection
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={'ICPL-24-25-LAPT'};DATABASE={'Energy Monitoring_Realtime'};Trusted_Connection=yes;"
)
cursor = conn.cursor()



# Get today's date
today = datetime.today().date()


# Split DataFrame into bulk data (up to yesterday) and row-wise data (from today)
bulk_data = df[df['Date'] < today]
rowwise_data = df[df['Date'] == today]



# Sort bulk_data by ID, Date, and Time in ascending order
bulk_data = bulk_data.sort_values(by=['ID', 'Date', 'Time'], ascending=[True, True, True])

# Insert the bulk data (up to yesterday) into the database
insert_query = """
    INSERT INTO Melting_Prod (
        ID, 
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

# Bulk insert - use executemany for bulk insertion
bulk_values = bulk_data[['ID', 'Station', 'Date', 'Time', 'HeatNo', 
                         'Furnace Temperature', 'Fe%', 'C%', 
                         'Cr%', 'Ni%','Cumulative Planned Metal (kg)','Cumulative Actual Metal (kg)']].values.tolist()
cursor.executemany(insert_query, bulk_values)
conn.commit()  # Commit the bulk insert

# Sort rowwise_data by ID, Date, and Time in ascending order
rowwise_data = rowwise_data.sort_values(by=['ID', 'Date', 'Time'], ascending=[True, True, True])

# Insert row-by-row data from today with a minute delay
for _, row in rowwise_data.iterrows():
    row_values = (
        row['ID'],
        row['Station'],
        row['Date'],
        row['Time'],
        row['HeatNo'],
        row['Furnace Temperature'],
        row['Fe%'],
        row['C%'],
        row['Cr%'],
        row['Ni%'],
        row['Cumulative Planned Metal (kg)'],
        row['Cumulative Actual Metal (kg)']
    )
    cursor.execute(insert_query, row_values)
    conn.commit()  # Commit after each row

    # Add a 1-minute delay between row inserts
    time.sleep(30)

# Close the connection
cursor.close()
conn.close()
