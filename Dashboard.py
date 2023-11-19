import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from config import SQL_USERNAME, SQL_PASSWORD, SQL_HOST, SQL_DATABASE
import plotly.express as px

engine = create_engine(f'mysql+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DATABASE}')

st.set_page_config(layout="wide",initial_sidebar_state="collapsed")
st.sidebar.header("Phonepe Plus Data Visualization and Exploration")
top_type=st.sidebar.selectbox("Type",("Transaction","User"),index=0)
top_year=st.sidebar.selectbox("Year",("2018","2019","2020","2021","2022","2023"),index=0)
top_q=st.sidebar.selectbox("Quater",("1","2","3","4"),index=0)
#col1,col2,col3=st.columns((3,3,3))

# #with col1:
#     #top_type=st.selectbox("Type",("Transaction","User"),index=0)
# with col2:
#     top_year=st.selectbox("Year",("2018","2019","2020","2021","2022","2023"),index=0)
# with col3:
#     top_q=st.selectbox("Quater",("1","2","3","4"),index=0)

def top_transaction():
    col4, col5, col6 = st.columns((3, 3, 3))
    total_tra_query = f"Select COALESCE(sum(transaction_count),0) as c,COALESCE(sum(transaction_amount),0) as a from top_transaction where year={top_year} and quarter={top_q}"
    top_tra_query = f"select COALESCE(CONCAT(state, ''), 'Null') as s from top_transaction where year={top_year} and quarter={top_q} group by state order by COALESCE(sum(transaction_amount),0) Desc "
    total_tra = pd.read_sql_query(total_tra_query, engine)
    top_tra = pd.read_sql_query(top_tra_query, engine)
    top_tra = top_tra['s'][0] if top_tra.empty == False else "-"
    col4.metric("Total Transaction Count", format(int(total_tra['c'][0]), ',d'))
    col5.metric("Total Transaction Amount", format(int(total_tra['a'][0]), ',d'))
    col6.metric("Top Transaction state", top_tra.capitalize())
    col7, col8 = st.columns((5, 5))

    with col7:
        top_count_query = f"select COALESCE(CONCAT(state, ''), 'Null') as State,sum(transaction_count) as 'Transcation Count' from top_transaction where year={top_year} and quarter={top_q} group by state order by state Desc Limit 10"
        total_top_count = pd.read_sql_query(top_count_query, engine)
        fig = px.bar(total_top_count, title="Top 10 State", x="State", y="Transcation Count")
        st.plotly_chart(fig, use_container_width=True)
    with col8:
        top_pin_query = f"select COALESCE(CONCAT(district_pincode, ''), 'Null') as Pincode,sum(transaction_count) as 'Transcation Count' from top_transaction where year={top_year} and quarter={top_q} group by district_pincode order by district_pincode Desc Limit 10"
        total_pin_count = pd.read_sql_query(top_pin_query, engine)
        fig1 = px.pie(total_pin_count,
                      names="Pincode",
                      values="Transcation Count",
                      title="Top 10 Pincode",
                      hole=0.5,  # This creates the donut effect. Adjust the hole size as needed.
                      color_discrete_sequence=px.colors.qualitative.Set3  # Set color sequence
                      )
        st.plotly_chart(fig1, use_container_width=True)

def top_user():
    col4, col5 = st.columns((3, 3))
    total_tra_query = f"Select COALESCE(sum(registeredUsers),0) as c from top_user where year={top_year} and quarter={top_q}"
    top_tra_query = f"select COALESCE(CONCAT(state, ''), 'Null') as s from top_user where year={top_year} and quarter={top_q} group by state order by COALESCE(sum(registeredUsers),0) Desc "
    total_tra = pd.read_sql_query(total_tra_query, engine)
    top_tra = pd.read_sql_query(top_tra_query, engine)
    top_tra = top_tra['s'][0] if top_tra.empty == False else "-"
    col4.metric("Total Registered users", format(int(total_tra['c'][0]), ',d'))
    col5.metric("Top Registered state", top_tra.capitalize())

    col7, col8 = st.columns((5, 5))

    with col7:
        top_count_query = f"select COALESCE(CONCAT(state, ''), 'Null') as State,sum(registeredUsers) as 'Register User' from top_user where year={top_year} and quarter={top_q} group by state order by state Desc Limit 10"
        total_top_count = pd.read_sql_query(top_count_query, engine)
        fig = px.bar(total_top_count, title="Top 10 State", x="State", y="Register User")
        st.plotly_chart(fig, use_container_width=True)
    with col8:
        top_pin_query = f"select COALESCE(CONCAT(district_pincode, ''), 'Null') as Pincode,sum(registeredUsers) as 'Register User' from top_user where year={top_year} and quarter={top_q} group by district_pincode order by district_pincode Desc Limit 10"
        total_pin_count = pd.read_sql_query(top_pin_query, engine)
        fig1 = px.pie(total_pin_count,
                      names="Pincode",
                      values="Register User",
                      title="Top 10 Pincode",
                      hole=0.5,  # This creates the donut effect. Adjust the hole size as needed.
                      color_discrete_sequence=px.colors.qualitative.Set3  # Set color sequence
                      )
        st.plotly_chart(fig1, use_container_width=True)

def agg_transation():
    # col4, col5, col6 = st.columns((3, 3, 3))
    # total_tra_query = f"Select COALESCE(sum(transaction_count),0) as c,COALESCE(sum(transaction_amount),0) as a from top_transaction where year={top_year} and quarter={top_q}"
    # top_tra_query = f"select COALESCE(CONCAT(state, ''), 'Null') as s from top_transaction where year={top_year} and quarter={top_q} group by state order by COALESCE(sum(transaction_amount),0) Desc "
    # total_tra = pd.read_sql_query(total_tra_query, engine)
    # top_tra = pd.read_sql_query(top_tra_query, engine)
    # top_tra = top_tra['s'][0] if top_tra.empty == False else "-"
    # col4.metric("Total Transaction Count", format(int(total_tra['c'][0]), ',d'))
    # col5.metric("Total Transaction Amount", format(int(total_tra['a'][0]), ',d'))
    # col6.metric("Top Transaction state", top_tra.capitalize())
    col7, col8 = st.columns((5, 5))

    with col7:
        agg_line_query = f"SELECT COALESCE(transaction_type, 'Null') as 'Transaction type', year, SUM(transaction_amount) as 'Transaction Amount' FROM aggregated_transaction GROUP BY year, transaction_type;"
        total_agg_line = pd.read_sql_query(agg_line_query, engine)
        fig1 = px.line(total_agg_line, x="year", y="Transaction Amount", color='Transaction type', markers=True, title='Total Transaction Amount Over Years by Transaction Type')
        st.plotly_chart(fig1, use_container_width=True)
    with col8:
        top_pin_query = f"SELECT COALESCE(transaction_type, 'Null') as 'Transaction type', year, SUM(transaction_count) FROM aggregated_transaction GROUP BY year, transaction_type;"
        total_pin_count = pd.read_sql_query(top_pin_query, engine)
        fig3 = px.line(total_pin_count, x='year', y='SUM(transaction_count)', color='Transaction type', markers=True,
                      labels={'SUM(transaction_count)': 'Total Transactions', 'year': 'Year'},
                      title='Total Transactions Over Years by Transaction Type')
        st.plotly_chart(fig3, use_container_width=True)

    top_count_query = f"SELECT COALESCE(state, 'Null') as State, SUM(transaction_count) as `Transaction Count`, COALESCE(transaction_type, 'Null') as `Transaction Type`, COALESCE(quarter, 'Null') as Quarter FROM aggregated_transaction WHERE year = {top_year} GROUP BY state, transaction_type, quarter LIMIT 1000;"
    total_top_count = pd.read_sql_query(top_count_query, engine)
    fig = px.bar(total_top_count, title="Transaction Distribution Across Transaction Types for Each Quarter", x="State",
                 y="Transaction Count", color="Transaction Type", facet_col="Quarter", barmode='stack')
    st.plotly_chart(fig, use_container_width=True)

def agg_user():
    col7, col8 = st.columns((5, 5))

    with col7:
        agg_line_query = f"SELECT user_brand, COUNT(user_brand) as user_count FROM aggregated_user where year={top_year} GROUP BY user_brand ORDER BY user_count DESC LIMIT 10;"
        total_agg_line = pd.read_sql_query(agg_line_query, engine)
        fig = px.bar(
    total_agg_line,
    x='user_count',
    y='user_brand',
    orientation='h',  # horizontal orientation
    title='User Brands Based on User Count',
    labels={'user_count': 'User Count', 'user_brand': 'User Brand'},
    width=800,
    height=500
)
        st.plotly_chart(fig, use_container_width=True)
    with col8:
        top_pin_query = f"SELECT state, year, quarter, user_brand, COUNT(user_brand) as user_count, AVG(user_percentage) as avg_percentage FROM aggregated_user where year={top_year} GROUP BY state, quarter, user_brand ;"
        total_pin_count = pd.read_sql_query(top_pin_query, engine)
        fig1 = px.pie(total_pin_count,
                      names="user_brand",
                      values="user_count",
                      title='User Distribution Among User Brands',
                      hole=0.5,  # This creates the donut effect. Adjust the hole size as needed.
                      color_discrete_sequence=px.colors.qualitative.Set3  # Set color sequence
                      )
        st.plotly_chart(fig1, use_container_width=True)

def map_transaction():
    col7, col8 = st.columns((5, 5))
    with col7:
        agg_line_query = f"select state, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_transaction where year ={top_year} and quarter ={top_q} group by state order by state"
        total_agg_line = pd.read_sql_query(agg_line_query, engine)
        df2 = pd.read_csv('Statenames.csv')
        total_agg_line.state = df2
        fig = px.choropleth(total_agg_line,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='state',
                            color='Total_amount',
                            color_continuous_scale='sunset',
                            title="Overall State Data - TRANSACTIONS AMOUNT")

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(geo=dict(bgcolor='#0e1117'))
        st.plotly_chart(fig, use_container_width=True)
    with col8:
        top_pin_query = f"select state, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_transaction where year = {top_year} and quarter = {top_q} group by state order by state"
        total_pin_count = pd.read_sql_query(top_pin_query, engine)
        df2 = pd.read_csv('Statenames.csv')
        total_pin_count.state = df2

        fig = px.choropleth(total_pin_count,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='state',
                            color='Total_Transactions',
                            color_continuous_scale='sunset',
                            title="Overall State Data - TRANSACTIONS COUNT")

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(geo=dict(bgcolor='#0e1117'))
        st.plotly_chart(fig, use_container_width=True)

def map_user():
    agg_line_query = f"select state, sum(registeredUsers) as Total_Users, sum(appOpens) as Total_Appopens from map_user where year = {top_year} and quarter = {top_q} group by state order by state"
    total_agg_line = pd.read_sql_query(agg_line_query, engine)
    df2 = pd.read_csv('Statenames.csv')
    total_agg_line.Total_Appopens = total_agg_line.Total_Appopens.astype(float)
    total_agg_line.state = df2
    fig = px.choropleth(total_agg_line,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='state',
                        color='Total_Appopens',
                        color_continuous_scale='sunset',
                        title='Total Users by State')

    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(geo=dict(bgcolor='#0e1117'))
    st.plotly_chart(fig, use_container_width=True)


if top_type=="Transaction":

    map_transaction()
    agg_transation()
    top_transaction()

if top_type=="User":
    map_user()
    agg_user()
    top_user()