import streamlit as st
from PIL import Image
import os
import json
from streamlit_option_menu import option_menu
import subprocess
import plotly.express as px
import pandas as pd
import sqlite3
import requests

#response = requests.get('https://api.github.com/repos/PhonePe/pulse')
#repo = response.json()
#clone_url = repo['clone_url']


#repo_name = "pulse"
#clone_dir = os.path.join(os.getcwd(), repo_name)

#subprocess.run(["git", "clone", clone_url, clone_dir], check=True)




#This is to direct the path to get the data as states

path1=r"/Users/priti/Downloads/pulse-master 2/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path1)


State_list = pd.DataFrame(Agg_state_list)


#This is to extract the data's to create a dataframe of Aggregated Transaction

clm1={'State':[], 'Year':[],'Quater':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in Agg_state_list:
    p_i=path1+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm1['Transaction_type'].append(Name)
              clm1['Transaction_count'].append(count)
              clm1['Transaction_amount'].append(amount)
              clm1['State'].append(i)
              clm1['Year'].append(j)
              clm1['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm1)



#This is to extract the data's to create a dataframe of Aggregated User
path2=r"/Users/priti/Downloads/pulse-master 2/data/aggregated/user/country/india/state/"
Agg_user_state_list=os.listdir(path2)

clm2={'State':[], 'Year':[],'Quater':[],'Brands':[], 'User_count':[], 'User_percentage':[]}
for i in Agg_user_state_list:
    p_i=path2+i+"/"
    Agg_yr=os.listdir(p_i)

    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            B=json.load(Data)

            try:
              for z in B['data']['usersByDevice']:
                Brand=z['brand']
                count=z['count']
                percentage=z['percentage']
                clm2['Brands'].append(Brand)
                clm2['User_count'].append(count)
                clm2['User_percentage'].append(percentage)
                clm2['State'].append(i)
                clm2['Year'].append(j)
                clm2['Quater'].append(int(k.strip('.json')))
            except:
              pass
#Succesfully created a dataframe
Agg_User=pd.DataFrame(clm2)

#This is to extract the data's to create a dataframe of Map Transaction
path3=r"/Users/priti/Downloads/pulse-master 2/data/map/transaction/hover/country/india/state/"
Map_trans_hover_state_list=os.listdir(path3)

clm3 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Map_trans_hover_state_list:
    p_i = path3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            H = json.load(Data)

            for z in H['data']['hoverDataList']:
                District = z['name']
                count = z["metric"][0]["count"]
                amount = z["metric"][0]["amount"]
                clm3["District"].append(District)
                clm3['Transaction_count'].append(count)
                clm3['Transaction_amount'].append(amount)
                clm3['State'].append(i)
                clm3['Year'].append(j)
                clm3['Quater'].append(int(k.strip('.json')))

# Succesfully created a dataframe
Map_Trans = pd.DataFrame(clm3)


#This is to extract the data's to create a dataframe of Map User
path4=r"/Users/priti/Downloads/pulse-master 2/data/map/user/hover/country/india/state/"
Map_user_state_list=os.listdir(path4)

clm4={'State':[], 'Year':[],'Quater':[],'District':[], 'Number_of_users':[]}
for i in Map_user_state_list:
    p_i=path4+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            M=json.load(Data)
            for z in M["data"]["hoverData"].items():
              district = z[0]
              registereduser = z[1]["registeredUsers"]
              clm4['District'].append(district)
              clm4['Number_of_users'].append(registereduser)
              clm4['State'].append(i)
              clm4['Year'].append(j)
              clm4['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Map_User=pd.DataFrame(clm4)


#This is to extract the data's to create a dataframe of Top Transaction
path5=r"/Users/priti/Downloads/pulse-master 2/data/top/transaction/country/india/state/"
Top_trans_state_list=os.listdir(path5)

clm5={'State':[], 'Year':[],'Quater':[],'District':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in Top_trans_state_list:
    p_i=path5+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)

        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            T=json.load(Data)

            for z in T['data']['pincodes']:
              Name = z['entityName']
              count = z['metric']['count']
              amount = z['metric']['amount']
              clm5['District'].append(Name)
              clm5['Transaction_count'].append(count)
              clm5['Transaction_amount'].append(amount)
              clm5['State'].append(i)
              clm5['Year'].append(j)
              clm5['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Top_Trans =pd.DataFrame(clm5)

#This is to extract the data's to create a dataframe of Top User
path6=r"/Users/priti/Downloads/pulse-master 2/data/top/user/country/india/state/"
Top_user_state_list=os.listdir(path6)

clm6 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Registered_users': []}
for i in Top_user_state_list:
    p_i = path6 + i + "/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            R = json.load(Data)
            for t in R['data']['districts']:
                Name = t['name']
                registeredUser = t['registeredUsers']

                clm6['State'].append(i)
                clm6['Year'].append(j)
                clm6['Quater'].append(int(k.strip('.json')))
                clm6['District'].append(Name)
                clm6['Registered_users'].append(registeredUser)

# Succesfully created a dataframe
Top_User = pd.DataFrame(clm6)



#converting all dataframes into CSV file
Agg_Trans.to_csv('Agg_Trans.csv', index = False)
Agg_User.to_csv('Agg_User.csv', index = False)
Map_Trans.to_csv('Map_Trans.csv', index = False)
Map_User.to_csv('Map_User.csv', index = False)
Top_Trans.to_csv('Top_trans.csv', index = False)
Top_User.to_csv('Top_user.csv', index = False)
State_list.to_csv('State_list.csv', index = False)



#CREATING CONNECTION WITH SQL SERVER
connection = sqlite3.connect("phonepe pulse.db")
cursor = connection.cursor()

Agg_Trans.to_sql('aggregated_transaction', connection, if_exists='replace')
Agg_User.to_sql('aggregated_user', connection, if_exists='replace')
Map_Trans.to_sql('map_transaction', connection, if_exists='replace')
Map_User.to_sql('map_user', connection, if_exists='replace')
Top_Trans.to_sql('top_transaction', connection, if_exists='replace')
Top_User.to_sql('top_user', connection, if_exists='replace')
State_list.to_sql('State_list', connection, if_exists='replace')


with st.sidebar:
    SELECT = option_menu("Menu", ["Home","Top Trends","Visualise Data","About"],
                icons=["house","graph-up-arrow","bar-chart-line", "exclamation-circle"],
                menu_icon= "menu-button-wide",
                default_index=0,
                styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                        "nav-link-selected": {"background-color": "#6F36AD"}}

    )

# MENU 1 - HOME
if SELECT == "Home":
    #st.image("Phonepe1.jpeg")
    st.markdown("# :violet[Data Visualization and Exploration]")
    st.markdown("## :violet[A User-Friendly Tool Using Streamlit and Plotly]")
    col1,col2 = st.columns([3,2],gap="medium")
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("### :violet[Domain :] Fintech")
        st.markdown("### :violet[Technologies used :] Github Cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.")
        st.markdown("### :violet[Overview :] In this streamlit web app you can visualize the phonepe pulse data and gain lot of insights on transactions, number of users, top 10 state, district, pincode and which brand has most number of users and so on. Bar charts, Pie charts and Geo map visualization are used to get some insights.")
    with col2:
        image = Image.open('Phonepe2.jpeg')

        st.image(image, caption='Sunrise by the mountains')
         #st.image("Phonepe2.jpeg")
        st.write("insert image")
# MENU 2 - TOP CHARTS
if SELECT == "Top Charts":
    st.markdown("## :violet[Top Charts]")
    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    colum1, colum2 = st.columns([1, 1.5], gap="large")
    with colum1:
        Year = st.slider("**Year**", min_value=2018, max_value=2022)
        Quarter = st.slider("Quarter", min_value=1, max_value=4)

    with colum2:
        st.info(
            """
            #### From this menu we can get insights like :
            - Overall ranking on a particular Year and Quarter.
            - Top 10 State, District, Pincode based on Total number of transaction and Total amount spent on phonepe.
            - Top 10 State, District, Pincode based on Total phonepe users and their app opening frequency.
            - Top 10 mobile brands and its percentage based on the how many people use phonepe.
            """, icon="🔍"
        )
    if Type == "Transactions":
        col1, col2, col3 = st.columns([1, 1, 1], gap="small")

        with col1:
            st.markdown("### :violet[State]")
            cursor.execute(
                f"select State, sum(Transaction_amount/Transaction_count) as Per_Capita_transaction from aggregated_transaction where Year = {Year} and Quater = {Quarter} group by State order by Per_Capita_transaction desc limit 10")
                #f"select State, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from aggregated_transaction where Year = {Year} and Quater = {Quarter} group by State order by Total desc limit 10")
            #df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transactions_Count', 'Total_Amount'])
            df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Per_Capita_transaction'])

            fig = px.pie(df, values='Per_Capita_transaction',
                         names='State',
                         title='Top 10',
                         color_discrete_sequence=px.colors.sequential.Agsunset,
                         hover_data=['Per_Capita_transaction'],
                         labels={'Per_Capita_transaction': 'Per_Capita_transaction'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### :violet[District]")
            cursor.execute(
                f"select District, sum(Transaction_amount/Transaction_count) as Per_Capita_transaction from map_transaction where Year = {Year} and Quater = {Quarter} group by District order by Per_Capita_transaction desc limit 10")
                #f"select district , sum(Count) as Total_Count, sum(Amount) as Total from Map_Trans where year = {Year} and quarter = {Quarter} group by district order by Total desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Per_Capita_transaction'])

            fig = px.pie(df, values='Per_Capita_transaction',
                         names='District',
                         title='Top 10',
                         color_discrete_sequence=px.colors.sequential.Agsunset,
                         hover_data=['Per_Capita_transaction'],
                         labels={'Per_Capita_transaction': 'Per_Capita_transaction'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.markdown("### :violet[Pincode]")
            cursor.execute(
                f"select District, sum(Transaction_amount/Transaction_count) as Per_Capita_transaction from top_transaction where Year = {Year} and Quater = {Quarter} group by District order by Per_Capita_transaction desc limit 10")
                #f"select pincode, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from Top_Trans where year = {Year} and quarter = {Quarter} group by pincode order by Total desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['Pincode', 'Per_Capita_transaction'])
            fig = px.pie(df, values='Per_Capita_transaction',
                         names='Pincode',
                         title='Top 10',
                         color_discrete_sequence=px.colors.sequential.Agsunset,
                         hover_data=['Per_Capita_transaction'],
                         labels={'Per_Capita_transaction': 'Per_Capita_transaction'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

    if Type == "Users":
        col1, col2, col3, col4 = st.columns([2, 2, 2, 2], gap="small")

        with col1:
            st.markdown("### :violet[Brands]")
            if Year == 2022 and Quarter in [2, 3, 4]:
                st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4")
            else:
                cursor.execute(
                    f"select Brands, sum(User_count) as Total_users, avg(User_percentage)*100 as Avg_Percentage from aggregated_user where Year = {Year} and Quater = {Quarter} group by Brands order by Total_users desc limit 10")
                    #f"select brands, sum(count) as Total_Count, avg(percentage)*100 as Avg_Percentage from agg_user where year = {Year} and quarter = {Quarter} group by brands order by Total_Count desc limit 10")
                df = pd.DataFrame(cursor.fetchall(), columns=['Brand', 'Total_Users', 'Avg_Percentage'])
                fig = px.bar(df,
                             title='Top 10',
                             x="Total_Users",
                             y="Brand",
                             orientation='h',
                             color='Avg_Percentage',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### :violet[District]")
            cursor.execute(
                f"select District, sum(Number_of_users) as Total_Users  from map_user where Year = {Year} and Quater = {Quarter} group by District order by Total_Users desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Total_Users'])
            df.Total_Users = df.Total_Users.astype(float)
            fig = px.bar(df,
                         title='Top 10',
                         x="Total_Users",
                         y="District",
                         orientation='h',
                         color='Total_Users',
                         color_continuous_scale=px.colors.sequential.Agsunset)
            st.plotly_chart(fig, use_container_width=True)
# MENU 3 - EXPLORE DATA
if SELECT == "Explore Data":
    Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2022)
    Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    col1,col2 = st.columns(2)
    # EXPLORE DATA - TRANSACTIONS
    if Type == "Transactions":
        # Overall State Data - TRANSACTIONS AMOUNT - INDIA MAP
        with col1:
            st.markdown("## :violet[Overall State Data - Transactions Amount]")
            cursor.execute(
                f"select State, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from map_transaction where Year = {Year} and Quater = {Quarter} group by State order by State")
            df1 = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Transactions', 'Total_amount'])
            #df2 = pd.read_csv('State_list.csv')
            #df1.State = df2

            fig = px.choropleth(df1,
                                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations='State',
                                color='Total_amount',
                                color_continuous_scale='sunset')

            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("## :violet[Overall State Data - Transactions Count]")
            cursor.execute(
                f"select State, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from map_transaction where Year = {Year} and Quater = {Quarter} group by State order by State")
            df1 = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Transactions', 'Total_amount'])
            #df2 = pd.read_csv('State_list.csv')
            #df1.Total_Transactions = df1.Total_Transactions.astype(int)
            #df1.State = df2

            fig = px.choropleth(df1,
                                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations='State',
                                color='Total_Transactions',
                                color_continuous_scale='sunset')

            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig, use_container_width=True)

    if Type == "Users":
        # Overall State Data - TOTAL APPOPENS - INDIA MAP
        st.markdown("## :violet[Overall State Data - User App opening frequency]")
        cursor.execute(
            f"select State, sum(Number_of_users) as Total_Users from map_user where Year = {Year} and Quater = {Quarter} group by State order by State")
        df1 = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Users'])
        #df2 = pd.read_csv('State_list.csv')
        #df1.Total_Appopens = df1.Total_Appopens.astype(float)
        #df1.State = df2

        fig = px.choropleth(df1,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Total_Users',
                            color_continuous_scale='sunset')

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig, use_container_width=True)









