import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import streamlit as st
import time

API_KEY = '1850243d5412420faeaaf737350b923a'

def fetch_news(country):
    url = 'https://newsapi.org/v2/top-headlines'
    params = {
        'apiKey': API_KEY,
        'country': country,
        'pageSize': 10,
        'language': 'en'
    }
    response = requests.get(url, params=params)
    data = response.json()
    articles = data.get('articles', [])
    df = pd.DataFrame(articles)
    if not df.empty:
        df['country'] = country
        df['fetch_time'] = datetime.now()
    return df

def clean_data(df):
    # Keep relevant columns and drop rows with missing titles or urls
    df = df[['source', 'author', 'title', 'description', 'url', 'publishedAt', 'country', 'fetch_time']]
    df = df.dropna(subset=['title', 'url'])
    # Flatten 'source' dict to just source name
    df['source'] = df['source'].apply(lambda x: x['name'] if isinstance(x, dict) else x)
    # Convert all column names to lowercase to match PostgreSQL's default behavior
    df.columns = [col.lower() for col in df.columns]
    return df

def load_to_postgres(df):
    engine = create_engine('postgresql+psycopg2://suniljoe@localhost/news_db')
    df.to_sql('news_headlines', engine, if_exists='append', index=False)
    return engine

def launch_dashboard(engine):
    st.title("Top 10 News Headlines - USA & Canada")
    query = """
    SELECT title, source, author, publishedat, url, country, fetch_time
    FROM news_headlines
    ORDER BY fetch_time DESC
    LIMIT 20
    """

    df = pd.read_sql(query, engine)
    for idx, row in df.iterrows():
        st.markdown(f"### [{row['title']}]({row['url']})")
        st.write(f"Source: {row['source']} | Author: {row['author']} | Published: {row['publishedat']} | Country: {row['country']}")
        st.write("---")

def main():
    st.write("Fetching news for USA...")
    df_us = fetch_news('us')
    st.write("Fetching news for Canada...")
    df_ca = fetch_news('ca')
    df = pd.concat([df_us, df_ca], ignore_index=True)
    if df.empty:
        st.error("No news data fetched.")
        return
    st.write("Cleaning data...")
    df = clean_data(df)
    st.write("Loading data into PostgreSQL...")
    engine = load_to_postgres(df)
    st.success("Data loaded successfully! Launching dashboard...")
    time.sleep(1)
    launch_dashboard(engine)

if __name__ == "__main__":
    # Run with: streamlit run news_pipeline_dashboard.py
    main()
