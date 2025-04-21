import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import pyodbc
from dash.dependencies import Input, Output
import plotly.express as px

# SQL Server Connection Details
server = 'ICPL-24-25-LAPT'
database = 'Energy Monitoring_Realtime'

# Fetching Latest Energy Data for Cards
def fetch_latest_energy_data():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Process, Power, Consumption, PowerFactor FROM Latest_AllEnergy_Readings_View"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Fetching Data for Line Chart
def fetch_power_view():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = """
    SELECT Date, Time, SUM(Power) AS Total_Power_KW
    FROM Last9_Energy_Readings_Vw
    GROUP BY Date, Time
    ORDER BY Date ASC, Time ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    df['Timestamp'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    return df

# Fetching Data for Daily Consumption Bar Chart
def fetch_daily_consumption():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Date, Total_Consumption FROM Daily_Consumption_View ORDER BY Date ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Fetching Data for Daily Production Line Chart
def fetch_daily_production():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Date, Daily_Production FROM Daily_Production_View ORDER BY Date ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Fetching Data for current power card card
def fetch_currentpower():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Round(SUM(Power),1) AS TotalPower FROM Latest_Energy_Reading_View"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["TotalPower"].notna().iloc[0]:
        return df["TotalPower"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available
    

# Fetching Data for TodayConsumption card
def Fetch_TodayConsumption():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Top 1 Round([Total_Consumption],1) AS TodayConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] ORDER BY Date Desc;"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["TodayConsumption"].notna().iloc[0]:
        return df["TodayConsumption"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available


# Fetching Data for TodayProduction card
def Fetch_TodayProduction():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT TOP 1 Round([Daily_Production],1) AS TodayProduction FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] ORDER BY Date Desc;"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["TodayProduction"].notna().iloc[0]:
        return df["TodayProduction"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available
    
# Fetching Data for ThisMonthConsumption card
def Fetch_ThisMonthConsumption():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Round(SUM([Total_Consumption]),1) AS ThisMonthConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE());"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["ThisMonthConsumption"].notna().iloc[0]:
        return df["ThisMonthConsumption"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available
    

# Fetching Data for PreviousMonthConsumption card
def Fetch_PreviousMonthConsumption():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT Round(SUM([Total_Consumption]),1) AS PreviousMonthConsumption FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(DATEADD(MONTH, -1, GETDATE())) AND MONTH(Date) = MONTH(DATEADD(MONTH, -1, GETDATE()));"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["PreviousMonthConsumption"].notna().iloc[0]:
        return df["PreviousMonthConsumption"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available
    

# Fetching Data for ThisMonthConsumptionPerTonne card
def Fetch_ThisMonthConsumptionPerTonne():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())) / (SELECT SUM([Daily_Production])/1000 FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE())),1) AS ThisMonthConsumptionPerTonne;"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["ThisMonthConsumptionPerTonne"].notna().iloc[0]:
        return df["ThisMonthConsumptionPerTonne"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available
    

# Fetching Data for PreviousMonthConsumptionPerTonne card
def Fetch_PreviousMonthConsumptionPerTonne():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    query = "SELECT ROUND((SELECT SUM([Total_Consumption]) FROM [Energy Monitoring_Realtime].[dbo].[Daily_Consumption_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1) / (SELECT SUM([Daily_Production])/1000 FROM [Energy Monitoring_Realtime].[dbo].[Daily_Production_View] WHERE YEAR(Date) = YEAR(GETDATE()) AND MONTH(Date) = MONTH(GETDATE()) - 1),1) AS PreviousMonthConsumptionPerTonne;"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure the result is not empty and extract the value
    if not df.empty and df["PreviousMonthConsumptionPerTonne"].notna().iloc[0]:
        return df["PreviousMonthConsumptionPerTonne"].iloc[0]
    else:
        return 0  # Default to 0 if no data is available

# Dash App Initialization
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.title = "Live Energy Dashboard"

# Card Layout Generator
def generate_card(process, power, consumption, power_factor):
    return dbc.Card(
        dbc.CardBody([
            html.H5(process, className="card-title text-center fw-bold mb-2"),  # Process Name
            
            # Row for Values
            dbc.Row([
                dbc.Col(html.H4(f"{power}", className="text-center fw-bold"), width=4),
                dbc.Col(html.H4(f"{consumption}", className="text-center fw-bold"), width=4),
                dbc.Col(html.H4(f"{power_factor}", className="text-center fw-bold"), width=4),
            ], className="mb-1"),
            
            # Row for Parameter Names
            dbc.Row([
                dbc.Col(html.P("Power (KW)", className="text-center small"), width=4),
                dbc.Col(html.P("Consumption (KVAH)", className="text-center small"), width=4),
                dbc.Col(html.P("Power Factor", className="text-center small"), width=4),
            ]),
        ]),
        className="text-white bg-primary shadow-lg rounded-3",
        style={'height': '130px',"background-color": "#09122C", "color": "white", "border": "0.2px solid gray"}  # Adjust height as needed
    )

# Function to generate the "Current Power Usage" card
def generate_current_power_card(TotalPower):
    return dbc.Card(
        dbc.CardBody([
            html.H4(f"{TotalPower} kW", className="text-center fw-bold"),
            html.P("Current Power Usage", className="text-center small"),
        ]),
        className="text-white bg-danger shadow-lg rounded-3",
        style={"height": "120px"}
    )
# Function to generate the "TodayConsumptionProduction" card
def generate_todayConsumptionProduction_card(TodayConsumption,TodayProduction):
    return dbc.Card(
        dbc.CardBody([
            html.H4(f"{TodayConsumption} kVAh", className="text-center fw-bold"),
            html.P("Today Consumption", className="text-center small"),
            html.H4(f"{TodayProduction} Tonne", className="text-center fw-bold"),
            html.P("Today Production", className="text-center small"),
        ]),
        className="text-white bg-danger shadow-lg rounded-3",
        style={"height": "120px"}
    )

# Function to generate the "TodayConsumptionProduction" card
def generate_ThisPreviousMonthConsumption_card(ThisMonthConsumption,PreviousMonthConsumption):
    return dbc.Card(
        dbc.CardBody([
            html.H4(f"{ThisMonthConsumption} kVAh", className="text-center fw-bold"),
            html.P("This Month Consumption", className="text-center small"),
            html.H4(f"{PreviousMonthConsumption} kVAh", className="text-center fw-bold"),
            html.P("Previous Month Consumption", className="text-center small"),
        ]),
        className="text-white bg-danger shadow-lg rounded-3",
        style={"height": "120px"}
    )

# Function to generate the "ConsumptionPerTonne" card
def generate_ThisPreviousMonthConsumptionPerTonne_card(ThisMonthConsumptionPerTonne,PreviousMonthConsumptionPerTonne):
    return dbc.Card(
        dbc.CardBody([
            html.H4(f"{ThisMonthConsumptionPerTonne} kVAh", className="text-center fw-bold"),
            html.P("This Month", className="text-center small"),
            html.H4(f"{PreviousMonthConsumptionPerTonne} kVAh", className="text-center fw-bold"),
            html.P("Previous Month", className="text-center small"),
        ]),
        className="text-white bg-danger shadow-lg rounded-3",
        style={"height": "120px"}
    )


app.layout = dbc.Container([
    html.H2("Real Time Energy Monitoring", className="text-center my-3 text-white"),
    
    # Station Cards
    dbc.Row(id="card-row-1", className="mb-3"),  # Row 1 with 4 Cards
    dbc.Row(id="card-row-2", className="mb-3"),  # Row 2 with 3 Cards
    
    # Real-Time Charts
    dbc.Row([
        dbc.Col(dcc.Graph(id='power-line-chart', style={'height': '350px',"background-color": "#09122C", "color": "white", "border": "0.2px solid gray"}), width=4),
        dbc.Col(dcc.Graph(id='daily-consumption-bar-chart', style={'height': '350px',"background-color": "#09122C", "color": "white", "border": "0.2px solid gray"}), width=4),
        dbc.Col(dcc.Graph(id='daily-consumption-per-tonne-chart', style={'height': '350px',"background-color": "#09122C", "color": "white", "border": "0.2px solid gray"}), width=4),
    ], className="mb-3",),  # Space between charts and KPI cards
    
    # Additional KPI Cards (Last Row)
    dbc.Row([
        # Group 1: Current Power Usage
        dbc.Col(dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.P("Power", className="d-inline-block me-3 small"),
                ],className="text-center"),
                html.H4(id="current-power", className="text-center fw-bold"),
                html.P("Current Power Usage", className="text-center small"),
            ]),
            className="text-white shadow-lg rounded-3",
            style={"height": "120px",
                   "background-color": "#09122C",  # Dark background
                    "color": "white",  # White text for contrast
                    "border": "0.2px solid gray"  # Red border for emphasis
                }
        ), width=3),

        # Group 2: Consumption Today & Production Today
        dbc.Col(dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.P("Consumption & Production", className="d-inline-block me-3 small"),
                ],className="text-center"),
                html.Div([
                    html.H4(id="consumption-today", className="text-center fw-bold d-inline-block me-3"),
                    html.H4(id="production-today", className="text-center fw-bold d-inline-block"),
                ], className="text-center"),
                html.Div([
                    html.P("Consumption Today", className="d-inline-block me-3 small"),
                    html.P("Production Today", className="d-inline-block small"),
                ], className="text-center"),
            ]),
            className="text-white shadow-lg rounded-3",
            style={"height": "120px",
                   "background-color": "#09122C",  # Dark background
                    "color": "white",  # White text for contrast
                    "border": "0.2px solid gray"  # Red border for emphasis
                }
        ), width=3),

        # Group 3: Monthly & Previous Month Consumption
        dbc.Col(dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.P("Consumption", className="d-inline-block me-3 small"),
                ],className="text-center"),
                html.Div([
                    html.H4(id="consumption-This-month", className="text-center fw-bold d-inline-block me-3"),
                    html.H4(id="consumption-prev-month", className="text-center fw-bold d-inline-block"),
                ], className="text-center"),
                html.Div([
                    html.P("This Month", className="d-inline-block me-3 small"),
                    html.P("Previous Month", className="d-inline-block small"),
                ], className="text-center"),
            ]),
            className="text-white shadow-lg rounded-3",
            style={"height": "120px",
                   "background-color": "#09122C",  # Dark background
                    "color": "white",  # White text for contrast
                    "border": "0.2px solid gray"  # Red border for emphasis
                }
        ), width=3),

        # Group 4: Consumption per Tonne (Current & Previous Month)
        dbc.Col(dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.P("Consumption Per Tonne", className="d-inline-block me-3 small"),
                ],className="text-center"),
                html.Div([
                    html.H4(id="consumption-per-tonne-this-month", className="text-center fw-bold d-inline-block me-3"),
                    html.H4(id="consumption-per-tonne-prev-month", className="text-center fw-bold d-inline-block"),
                ], className="text-center"),
                html.Div([
                    html.P("This Month", className="d-inline-block me-3 small"),
                    html.P("Previous Month", className="d-inline-block small"),
                ], className="text-center"),
            ]),
            className="text-white shadow-lg rounded-3",
            style={"height": "120px",
                   "background-color": "#09122C",  # Dark background
                    "color": "white",  # White text for contrast
                    "border": "0.2px solid gray"  # Red border for emphasis
                }
        ), width=3),
    ]),

    # Interval Components for Live Updates
    dcc.Interval(id="interval-card", interval=15000, n_intervals=0),  # 15 Sec for Cards
    dcc.Interval(id='interval-line', interval=15000, n_intervals=0),  # 15 Sec for Line Chart
    dcc.Interval(id='interval-bar', interval=15000, n_intervals=0)   # 15 sec for Bar Chart
], fluid=True)


# Callback for Live Cards
@app.callback(
    [Output("card-row-1", "children"), Output("card-row-2", "children")],
    Input("interval-card", "n_intervals")
)
def update_cards(_):
    df = fetch_latest_energy_data()
    
    # Create Cards
    cards = [generate_card(row["Process"], row["Power"], row["Consumption"], row["PowerFactor"]) 
             for _, row in df.iterrows()]
    
    # Layout: 4 Cards in Row 1, 3 Cards in Row 2
    row1 = [dbc.Col(cards[i], width=3) for i in range(4)]
    row2 = [dbc.Col(cards[i], width=4) for i in range(4, 7)]
    
    return row1, row2

@app.callback(
    Output("current-power", "children"),
    Input("interval-card", "n_intervals")
)
def update_current_power(n):
    # Fetch the latest power data from your database or source
    TotalPower = fetch_currentpower()  # Replace with actual data fetching function round(TotalPower.iloc[0, 0], 2)
    return f"{TotalPower} kW"  # Updating the value inside the card

@app.callback(
    Output("consumption-today", "children"),
    Input("interval-card", "n_intervals")
)
def update_Today_Consumption(n):
    # Fetch the latest power data from your database or source
    TodayConsumption = Fetch_TodayConsumption()  # Replace with actual data fetching function
    return f"{TodayConsumption} kVAh"  # Updating the value inside the card

@app.callback(
    Output("production-today", "children"),
    Input("interval-card", "n_intervals")
)
def update_Today_Production(n):
    # Fetch the latest power data from your database or source
    TodayProduction = Fetch_TodayProduction()  # Replace with actual data fetching function
    return f"{TodayProduction} Tonne"  # Updating the value inside the card

@app.callback(
    Output("consumption-This-month", "children"),
    Input("interval-card", "n_intervals")
)
def update_ThisMonth_Consumption(n):
    # Fetch the latest power data from your database or source
    ThisMonthConsumption = Fetch_ThisMonthConsumption()  # Replace with actual data fetching function
    return f"{ThisMonthConsumption} kVAh"  # Updating the value inside the card

@app.callback(
    Output("consumption-prev-month", "children"),
    Input("interval-card", "n_intervals")
)
def update_PreviousMonth_Consumption(n):
    # Fetch the latest power data from your database or source
    PreviousMonthConsumption = Fetch_PreviousMonthConsumption()  # Replace with actual data fetching function
    return f"{PreviousMonthConsumption} kVAh"  # Updating the value inside the card

@app.callback(
    Output("consumption-per-tonne-this-month", "children"),
    Input("interval-card", "n_intervals")
)
def update_ThisMonth_ConsumptionPerTonne(n):
    # Fetch the latest power data from your database or source
    ThisMonthConsumptionPerTonne = Fetch_ThisMonthConsumptionPerTonne()  # Replace with actual data fetching function
    return f"{ThisMonthConsumptionPerTonne} kVAh"  # Updating the value inside the card

@app.callback(
    Output("consumption-per-tonne-prev-month", "children"),
    Input("interval-card", "n_intervals")
)
def update_PreviousMonth_ConsumptionPerTonne(n):
    # Fetch the latest power data from your database or source
    PreviousMonthConsumptionPerTonne = Fetch_PreviousMonthConsumptionPerTonne()  # Replace with actual data fetching function
    return f"{PreviousMonthConsumptionPerTonne} kVAh"  # Updating the value inside the card

# Callback for Live Line Chart
@app.callback(
    Output('power-line-chart', 'figure'),
    Input('interval-line', 'n_intervals')
)
def update_chart(n):
    df_power = fetch_power_view()

    if df_power.empty:
        return px.line(title="No Data Available")

    fig = px.line(
        df_power, 
        x='Timestamp', 
        y='Total_Power_KW', 
        title="Current Power (kW) by Date & Time",
        markers=True
    )

    # Dark Theme Adjustments
    fig.update_layout(
        template="plotly_dark",  # Dark theme
        plot_bgcolor="black",    # Background color
        paper_bgcolor="black",
        font=dict(color="white"),  # Font color
        xaxis=dict(
            autorange=True,  
            tickformat="%H:%M",
            showgrid=True,  # Enable gridlines
            gridcolor="gray",  # Gridline color
            gridwidth=1,  # Gridline thickness
            showline=False,
            linewidth=1,
            linecolor="gray",
            zeroline=True,  # Remove zero line
        ),
        yaxis=dict(
            title="kW",
            showgrid=True,
            gridcolor="gray",
            gridwidth=1,
            zeroline=True
        ),
        xaxis_title="Time",
        margin=dict(t=40, b=40, l=40, r=40)  # Padding around the chart
    )

    # Apply Dashed Gridlines with Opacity
    fig.update_xaxes(gridcolor="gray", griddash="dash", gridwidth=1, showgrid=False)
    fig.update_yaxes(gridcolor="gray", griddash="dash", gridwidth=0.6, showgrid=True)

    # Add Data Labels
    fig.update_traces(
        text=df_power['Total_Power_KW'].round(1),  # Rounded labels
        textposition="top center",
        mode='lines+markers+text',  # Show both markers and text
        textfont=dict(size=11, color="white"),
        opacity=1  # Adjust line opacity
    )

    return fig

# Callback for Daily Consumption Bar Chart with Production Line
@app.callback(
    Output('daily-consumption-bar-chart', 'figure'),
    Input('interval-bar', 'n_intervals')
)
def update_daily_consumption_chart(n):
    df_consumption = fetch_daily_consumption()
    df_production = fetch_daily_production()

    if df_consumption.empty or df_production.empty:
        return px.bar(title="No Data Available")

    # Merge DataFrames on Date
    df = pd.merge(df_consumption, df_production, on='Date', how='left')

    # Filter Last 10 Days Data
    df = df.sort_values('Date').tail(10)

    # Create Bar Chart for Daily Consumption
    fig = px.bar(
        df,
        x='Date',
        y='Total_Consumption',
        title="Daily Consumption wrt Production",
        labels={'Total_Consumption': 'Consumption (KVAH)', 'Date': 'Date'},
        text_auto='.1f'  # Format data labels (1 decimal)
    )

    # Add Line for Daily Production
    fig.add_scatter(
        x=df['Date'],
        y=df['Daily_Production'],
        mode='lines+markers',
        name='Daily Production',
        line=dict(color='orange', width=3)
    )
    
    # Update Layout with Dark Theme & Legend Adjustments
    fig.update_layout(
        template="plotly_dark",  # Dark theme
        plot_bgcolor="black",    
        paper_bgcolor="black",
        font=dict(color="white"),
        xaxis=dict(
            title="Date",
            tickformat="%d-%m-%y",
            showgrid=False,
            gridcolor="black"
        ),
        yaxis=dict(
            title="kVAh",
            showgrid=True,
            gridcolor="black"
        ),
        legend=dict(
            title=" ",
            orientation="h",  # Horizontal legend
            yanchor="bottom",
            y=-0.4,  # Move legend below chart
            xanchor="center",
            x=0.5
        ),
        margin=dict(t=40, b=40, l=40, r=40),  # Padding around the chart
        bargap=0.4,  # Adjust space between bars (0 to 1)
        bargroupgap=0.1  # Adjust gap between grouped bars
    )

    return fig


# Callback for Daily Consumption per Tonne
@app.callback(
    Output('daily-consumption-per-tonne-chart', 'figure'),
    Input('interval-bar', 'n_intervals')
)
def update_daily_consumption_per_tonne_chart(n):
    df_consumption = fetch_daily_consumption()
    df_production = fetch_daily_production()

    if df_consumption.empty or df_production.empty:
        return px.line(title="No Data Available")

    # Merge DataFrames on Date
    df = pd.merge(df_consumption, df_production, on='Date', how='left')

    # Avoid division by zero
    df['Daily_kVAh_per_Tonne'] = df['Total_Consumption'] / (df['Daily_Production'] / 1000)
    df.replace([float('inf'), -float('inf')], None, inplace=True)

    # Filter last 10 days
    df = df.sort_values(by='Date').tail(10)

    # Create Line Chart
    fig = px.line(
        df, x='Date', y='Daily_kVAh_per_Tonne',
        title="Daily Energy Consumption per Tonne",
        labels={'Daily_kVAh_per_Tonne': 'kVAh per Tonne', 'Date': 'Date'},
        markers=True
    )

    # Dark Theme Adjustments
    fig.update_layout(
        template="plotly_dark",  # Dark theme
        plot_bgcolor="black",
        paper_bgcolor="black",
        font=dict(color="white"),
        xaxis=dict(
            title="Date",
            tickformat="%d-%m-%y",
            showgrid=True,
            gridcolor="gray",
            griddash="dash",
            gridwidth=1
        ),
        yaxis=dict(
            title="kVAh per Tonne",
            showgrid=True,
            gridcolor="gray",
            griddash="dash",
            gridwidth=1
        ),
        legend=dict(title="Metrics"),
        margin=dict(t=40, b=40, l=40, r=40)  # Padding around the chart
    )

    # Apply Dashed Gridlines with Opacity
    fig.update_xaxes(gridcolor="gray", griddash="dash", gridwidth=1, showgrid=False)
    fig.update_yaxes(gridcolor="gray", griddash="dash", gridwidth=0.3, showgrid=True)

    # Add Data Labels
    fig.update_traces(
        text=df['Daily_kVAh_per_Tonne'].round(1),  # Rounded labels
        textposition="top center",
        mode='lines+markers+text',  # Show markers, lines, and text
        textfont=dict(size=11, color="white"),
        opacity=1  # Adjust line opacity
    )

    return fig

if __name__ == "__main__":
    app.run(debug=True)


