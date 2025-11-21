import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sqlite3

# --- Configuration & Data Loading ---
st.set_page_config(layout="wide")
st.title("🌎 Global Country Data Analysis")

@st.cache_data
def load_data():
    """Connects to SQLite, reads the data, and prepares columns."""
    conn = sqlite3.connect('Country_Data.db')
    df = pd.read_sql_query("SELECT * FROM country_table", conn)
    conn.close()
    
    # Prepare derived columns required for plotting
    df['density'] = df['population'] / df['area_sqkm']
    df['log_population'] = np.log1p(df['population'])
    df['log_area_sqkm'] = np.log1p(df['area_sqkm'])
    return df

df = load_data()

st.sidebar.success(f"Loaded {len(df)} records from SQLite.")
st.header("📈 Five Key Analyses")

# --- Plotting Functions ---

def plot_histogram(data):
    fig, ax = plt.subplots()
    sns.histplot(data['log_population'], bins=20, kde=True, color='teal', ax=ax)
    ax.set_title('1. Population Distribution (Log Scale)')
    ax.set_xlabel('Log(Population)')
    return fig

def plot_bar_chart(data):
    regional_pop = data.groupby('region')['population'].mean().sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(7, 4))
    sns.barplot(x=regional_pop.index, y=regional_pop.values, palette="viridis", ax=ax)
    ax.set_title('2. Average Population by Region')
    ax.set_xticklabels(regional_pop.index, rotation=45, ha='right')
    ax.set_ylabel('Avg. Population')
    return fig

def plot_boxplot(data):
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.boxplot(data['density'].dropna())
    ax.set_title('3. Population Density Distribution')
    ax.set_xticklabels(['Density'])
    ax.set_ylabel('Density (people/sq km)')
    return fig

def plot_scatter(data):
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.scatterplot(x='log_area_sqkm', y='log_population', data=data, hue='region', alpha=0.7, ax=ax, legend=False)
    ax.set_title('4. Area vs. Population (Log Scale)')
    ax.set_xlabel('Log(Area)')
    ax.set_ylabel('Log(Population)')
    return fig

def plot_pie_chart(data):
    un_pop_sum = data.groupby('is_un_member')['population'].sum()
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(un_pop_sum, labels=['Non-Member', 'UN Member'], autopct='%1.1f%%', startangle=90)
    ax.set_title('5. Total Population by UN Membership')
    return fig

# --- Display Plots in Streamlit ---

col1, col2 = st.columns(2)
with col1:
    st.pyplot(plot_histogram(df))
    st.pyplot(plot_boxplot(df))
    st.pyplot(plot_pie_chart(df))
with col2:
    st.pyplot(plot_bar_chart(df))
    st.pyplot(plot_scatter(df))

st.markdown("---")
st.subheader("Raw Data Sample")
st.dataframe(df.head())