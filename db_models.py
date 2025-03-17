from sqlalchemy import Column, String, Integer, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from datetime import datetime
import os

# Create SQLAlchemy base
Base = declarative_base()

# Define Tweet model
class Tweet(Base):
    __tablename__ = 'tweets'

    id = Column(String, primary_key=True)
    text = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    company = Column(String, nullable=False)
    sentiment_score = Column(Float, nullable=False)
    sentiment_label = Column(String, nullable=False)
    sentiment_confidence = Column(Float)
    
    # User information
    user_username = Column(String, nullable=False)
    user_name = Column(String, nullable=False)
    user_profile_image_url = Column(String)
    user_followers_count = Column(Integer)
    
    # Metrics
    retweet_count = Column(Integer, default=0)
    reply_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    quote_count = Column(Integer, default=0)
    
    # hashtags
    hashtags = Column(String, default="")

    def __repr__(self):
        return f"<Tweet(id='{self.id}', company='{self.company}', sentiment='{self.sentiment_label}')>"


# Database connection and session setup
def get_db_connection():
    # Get database connection details from environment variables
    # For Supabase, you'll need the connection string in this format:
    # postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
    
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    
    return engine, Session


# Function to create tables if they don't exist
def create_tables():
    engine, _ = get_db_connection()
    Base.metadata.create_all(engine)
    print("Database tables created successfully")


# Function to upsert tweets from a DataFrame
def upsert_tweets_from_df(df):
    engine, _ = get_db_connection()
    
    try:
        # Process DataFrame in batches to avoid memory issues
        batch_size = 1000
        total_tweets = len(df)
        processed = 0
        
        # Flatten the nested structures in the DataFrame
        flattened_df = pd.DataFrame()
        
        # Copy basic columns
        flattened_df['id'] = df['id']
        flattened_df['text'] = df['text']
        flattened_df['created_at'] = pd.to_datetime(df['created_at'])
        flattened_df['company'] = df['company']
        
        # Extract sentiment columns
        flattened_df['sentiment_score'] = df['sentiment'].apply(lambda x: x['score'])
        flattened_df['sentiment_label'] = df['sentiment'].apply(lambda x: x['label'])
        flattened_df['sentiment_confidence'] = df['sentiment'].apply(lambda x: x.get('confidence'))
        
        # Extract user columns
        flattened_df['user_username'] = df['user'].apply(lambda x: x['username'])
        flattened_df['user_name'] = df['user'].apply(lambda x: x['name'])
        flattened_df['user_profile_image_url'] = df['user'].apply(lambda x: x['profile_image_url'])
        flattened_df['user_followers_count'] = df['user'].apply(lambda x: x['followers_count'])
        
        # Extract metrics columns
        flattened_df['retweet_count'] = df['metrics'].apply(lambda x: x['retweet_count'])
        flattened_df['reply_count'] = df['metrics'].apply(lambda x: x['reply_count'])
        flattened_df['like_count'] = df['metrics'].apply(lambda x: x['like_count'])
        flattened_df['quote_count'] = df['metrics'].apply(lambda x: x['quote_count'])
        
        # Extract hashtags from entities if available, otherwise use empty string
        def extract_hashtags(row):
            if 'entities' in row and row['entities'] and 'hashtags' in row['entities']:
                hashtags = row['entities']['hashtags']
                if hashtags and len(hashtags) > 0:
                    return ' '.join([f"#{tag}" for tag in hashtags])
            return ""
        
        flattened_df['hashtags'] = df.apply(extract_hashtags, axis=1)
        
        # Process in batches
        for i in range(0, total_tweets, batch_size):
            batch_df = flattened_df.iloc[i:i+batch_size]
            
            # Use to_sql with method='multi' for better performance
            # The 'replace' method doesn't work for upserts, so we'll use a custom function
            temp_table_name = 'temp_tweets'
            batch_df.to_sql(temp_table_name, engine, if_exists='replace', index=False)
            
            # Perform the upsert using a SQL query
            with engine.connect() as conn:
                # Start a transaction
                trans = conn.begin()
                try:
                    # Perform the upsert
                    conn.execute(f"""
                        INSERT INTO tweets (
                            id, text, created_at, company, 
                            sentiment_score, sentiment_label, sentiment_confidence,
                            user_username, user_name, user_profile_image_url, user_followers_count,
                            retweet_count, reply_count, like_count, quote_count, hashtags
                        )
                        SELECT 
                            id, text, created_at, company, 
                            sentiment_score, sentiment_label, sentiment_confidence,
                            user_username, user_name, user_profile_image_url, user_followers_count,
                            retweet_count, reply_count, like_count, quote_count, hashtags
                        FROM {temp_table_name}
                        ON CONFLICT (id) DO UPDATE SET
                            text = EXCLUDED.text,
                            created_at = EXCLUDED.created_at,
                            company = EXCLUDED.company,
                            sentiment_score = EXCLUDED.sentiment_score,
                            sentiment_label = EXCLUDED.sentiment_label,
                            sentiment_confidence = EXCLUDED.sentiment_confidence,
                            user_username = EXCLUDED.user_username,
                            user_name = EXCLUDED.user_name,
                            user_profile_image_url = EXCLUDED.user_profile_image_url,
                            user_followers_count = EXCLUDED.user_followers_count,
                            retweet_count = EXCLUDED.retweet_count,
                            reply_count = EXCLUDED.reply_count,
                            like_count = EXCLUDED.like_count,
                            quote_count = EXCLUDED.quote_count,
                            hashtags = EXCLUDED.hashtags
                    """)
                    
                    # Drop the temporary table
                    conn.execute(f"DROP TABLE {temp_table_name}")
                    
                    # Commit the transaction
                    trans.commit()
                except Exception as e:
                    # Rollback in case of error
                    trans.rollback()
                    raise e
            
            processed += len(batch_df)
            print(f"Processed {processed}/{total_tweets} tweets")
        
        print("All tweets have been successfully upserted into the database")
        return total_tweets
        
    except Exception as e:
        print(f"Error upserting tweets: {str(e)}")
        raise


# Function to load tweets from a DataFrame and upsert them
def load_and_upsert_df(df):
    # Create tables if they don't exist
    create_tables()
    
    # Upsert tweets from the DataFrame
    return upsert_tweets_from_df(df)
