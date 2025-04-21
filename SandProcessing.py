import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from datetime import datetime
import time
import sqlalchemy

# Parameters
start_date = datetime(2025, 3, 1)
end_date = datetime(2025, 5, 20)
working_hours = range(9, 18)  # 9 AM to 6 PM
days_of_week = [0, 1, 2, 3, 4, 5]  # Monday to Saturday
prob_status = [0.8, 0.15, 0.05]  # Probabilities for Working, Idle, Maintenance

# Data generation
timestamps = []
pf_values = []
power_values = []
reading_values = []
consumption_values = []
statuses = []
notifications = []

current_kvah_reading = 0

# Function to generate random PF with probabilities
def generate_pf():
    rand = np.random.rand()
    if rand < 0.07:  # 7% Low PF occational
        return round(np.random.uniform(0.79, 0.84), 2)
    elif rand < 0.09:  # 2% High PF Rare
        return round(np.random.uniform(0.96, 0.99), 2)
    else:  # Normal PF
        return round(np.random.uniform(0.85, 0.95), 2)
    
# Generate timestamps for working days and hours
current_date = start_date
while current_date <= end_date:
    if current_date.weekday() in days_of_week:
        for hour in working_hours:
            for minute in range(60):
                timestamps.append(current_date.replace(hour=hour, minute=minute, second=0))
    current_date += timedelta(days=1)


# Generate data for each timestamp
for ts in timestamps:
    pf = generate_pf()
    power = np.random.uniform(5, 10)  # Default to Working
    status = np.random.choice(["Working", "Idle", "Maintenance"], p=prob_status)

    if status == "Idle":
        power = np.random.uniform(0.5, 1)
    elif status == "Maintenance":
        power = 0

    consumption = power * (1 / 60)  # Convert power (KW) to KVAH for 1 minute
    current_kvah_reading += consumption

    notification = "Normal PF"
    if pf < 0.85:
        notification = "Low PF"
    elif pf > 0.95:
        notification = "High PF"

        # Append data
    pf_values.append(pf)
    power_values.append(round(power, 2))
    reading_values.append(round(current_kvah_reading, 2))
    consumption_values.append(round(consumption, 2))
    statuses.append(status)
    notifications.append(notification)


# Create DataFrame
df = pd.DataFrame({
    "Timestamp": timestamps,
    "Power Factor": pf_values,
    "Power (KW)": power_values,
    "Reading (KVAH)": reading_values,
    "Consumption (KVAH)": consumption_values,
    "Machine Status": statuses,
    "Notification": notifications
})


# Add ID column as row numbers (starting from 1)
df['ID'] = range(1, len(df) + 1)

# Add Index column with constant value "Sand_Energy"
df['Station'] = "Sand_Energy"

# Split Timestamp into separate Date and Time columns
df['Date'] = df['Timestamp'].dt.date
df['Time'] = df['Timestamp'].dt.time

# Drop the original Timestamp column (if not required)
df.drop(columns=['Timestamp'], inplace=True)

# Reorder columns for better readability
df = df[['ID', 'Station', 'Date', 'Time', 'Power Factor', 'Power (KW)', 'Reading (KVAH)', 'Consumption (KVAH)', 'Machine Status', 'Notification']]


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
    INSERT INTO SandProcessing_Energy (
        ID, 
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

# Bulk insert - use executemany for bulk insertion
bulk_values = bulk_data[['ID', 'Station', 'Date', 'Time', 'Power Factor', 
                         'Power (KW)', 'Reading (KVAH)', 'Consumption (KVAH)', 
                         'Machine Status', 'Notification']].values.tolist()
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
        row['Power Factor'],
        row['Power (KW)'],
        row['Reading (KVAH)'],
        row['Consumption (KVAH)'],
        row['Machine Status'],
        row['Notification']
    )
    cursor.execute(insert_query, row_values)
    conn.commit()  # Commit after each row

    # Add a 1-minute delay between row inserts
    time.sleep(30)

# Close the connection
cursor.close()
conn.close()
