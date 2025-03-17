import os
import sys
import pandas as pd
from db_models import load_and_upsert_df

def main():
    # Check if DATABASE_URL is set
    if not os.environ.get('DATABASE_URL'):
        print("ERROR: DATABASE_URL environment variable is not set.")
        print("Please set it to your Supabase PostgreSQL connection string:")
        print("Example: postgresql://postgres:[PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres")
        print("\nYou can set it temporarily for this session with:")
        print("export DATABASE_URL='postgresql://postgres:[PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres'")
        sys.exit(1)
    
    # Check if the CSV file exists
    csv_file_path = 'mock_tweets.csv'
    json_file_path = 'mock_tweets.json'
    
    if os.path.exists(csv_file_path):
        print(f"Loading tweets from CSV file: {csv_file_path}")
        # Load the DataFrame from CSV
        tweets_df = pd.read_csv(csv_file_path)
        
        # Convert string representations of dictionaries to actual dictionaries
        for col in ['sentiment', 'user', 'metrics', 'entities']:
            if col in tweets_df.columns:
                if col == 'entities' and tweets_df[col].isna().all():
                    # Skip if all values are NaN
                    continue
                tweets_df[col] = tweets_df[col].apply(eval)
    
    elif os.path.exists(json_file_path):
        print(f"Loading tweets from JSON file: {json_file_path}")
        # Load the DataFrame from JSON
        tweets_df = pd.read_json(json_file_path, orient='records')
    
    else:
        print(f"ERROR: Neither {csv_file_path} nor {json_file_path} found.")
        print("Please run generate_mock_tweets.py first to create the mock data.")
        sys.exit(1)
    
    print("Starting to load tweets into the database...")
    try:
        count = load_and_upsert_df(tweets_df)
        print(f"Successfully loaded {count} tweets into the database.")
    except Exception as e:
        print(f"Error loading tweets: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
