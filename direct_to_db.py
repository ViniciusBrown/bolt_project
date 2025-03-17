import os
import sys
from generate_mock_tweets import generate_all_tweets
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
    
    print("Generating mock tweet data...")
    tweets_df = generate_all_tweets(num_tweets_per_company=5000)
    print(f"Generated {len(tweets_df)} tweets.")
    
    print("Starting to load tweets directly into the database...")
    try:
        count = load_and_upsert_df(tweets_df)
        print(f"Successfully loaded {count} tweets into the database.")
    except Exception as e:
        print(f"Error loading tweets: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
