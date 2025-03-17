import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import uuid

# Set random seed for reproducibility
np.random.seed(42)

# Define companies
companies = [
    {
        "name": "Apple Inc.",
        "hashtags": ["Apple", "iPhone", "MacBook", "iOS", "iPad", "AirPods", "AppleWatch"],
        "topics": ["iPhone 14", "iOS 16", "MacBook Pro", "Apple Watch", "AirPods"],
        "positive_phrases": [
            "love my new {product}",
            "amazing experience with {product}",
            "best {product} ever",
            "{product} is incredible",
            "impressed with the {product}",
            "customer service is top notch",
            "worth every penny",
            "can't believe how good {product} is",
            "exceeded my expectations",
            "game changer"
        ],
        "negative_phrases": [
            "disappointed with {product}",
            "terrible experience with {product}",
            "{product} keeps crashing",
            "overpriced for what you get",
            "customer service was unhelpful",
            "wouldn't recommend {product}",
            "waste of money",
            "having issues with my {product}",
            "expected better from {company}",
            "going back to the competition"
        ]
    },
    {
        "name": "Tesla, Inc.",
        "hashtags": ["Tesla", "ElonMusk", "EV", "ModelY", "ModelS", "ModelX", "Model3", "Cybertruck", "FSD"],
        "topics": ["Model Y", "Full Self-Driving", "Elon Musk", "Cybertruck", "Supercharger"],
        "positive_phrases": [
            "love my new {product}",
            "autopilot is amazing",
            "best car I've ever owned",
            "supercharger network is fantastic",
            "software update improved everything",
            "acceleration is mind-blowing",
            "saving so much on gas",
            "best decision I ever made",
            "the future of driving",
            "zero emissions and loving it"
        ],
        "negative_phrases": [
            "service center delays are frustrating",
            "quality control issues with my {product}",
            "FSD still not fully working",
            "panel gaps on my new {product}",
            "waiting too long for repairs",
            "range anxiety is real",
            "autopilot disengaged unexpectedly",
            "price increases are ridiculous",
            "delivery was delayed again",
            "software update broke features"
        ]
    },
    {
        "name": "Microsoft",
        "hashtags": ["Microsoft", "Windows11", "Office365", "Teams", "Xbox", "Azure", "Surface"],
        "topics": ["Windows 11", "Microsoft Teams", "Xbox", "Office 365", "Azure"],
        "positive_phrases": [
            "Windows 11 is a great improvement",
            "Teams has transformed our workflow",
            "Xbox Game Pass is the best value in gaming",
            "Office 365 makes collaboration so easy",
            "Azure services are rock solid",
            "Surface laptop is beautifully designed",
            "PowerBI has changed how we use data",
            "Microsoft's accessibility features are industry-leading",
            "seamless integration between services",
            "Microsoft has really turned things around"
        ],
        "negative_phrases": [
            "Windows 11 update broke my computer",
            "Teams keeps crashing during meetings",
            "too many bugs in the latest release",
            "customer support couldn't solve my issue",
            "forced updates are so annoying",
            "subscription model is too expensive",
            "OneDrive sync issues are frustrating",
            "Windows search is still terrible",
            "too many service outages lately",
            "privacy concerns with data collection"
        ]
    }
]

# Define user profile image URLs from Unsplash
profile_images = [
    "https://images.unsplash.com/photo-1535713875002-d1d0cf377fde?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1494790108377-be9c29b29330?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1570295999919-56ceb5ecca61?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1568602471122-7832951cc4c5?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1599566150163-29194dcaad36?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1438761681033-6461ffad8d80?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1500648767791-00dcc994a43e?q=80&w=100&auto=format&fit=crop",
    "https://images.unsplash.com/photo-1607746882042-944635dfe10e?q=80&w=100&auto=format&fit=crop"
]

# Generate random usernames
def generate_username():
    adjectives = ["happy", "tech", "digital", "social", "cyber", "online", "web", "cloud", "smart", "future"]
    nouns = ["user", "fan", "guru", "ninja", "expert", "enthusiast", "lover", "pro", "master", "geek"]
    numbers = ["", str(random.randint(1, 999)), str(random.randint(1, 99))]
    return random.choice(adjectives) + random.choice(nouns) + random.choice(numbers)

# Generate random names
def generate_name():
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", 
                   "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah", "Thomas", "Karen", "Charles", "Nancy",
                   "Emma", "Olivia", "Noah", "Liam", "Sophia", "Ava", "Jackson", "Aiden", "Lucas", "Chloe"]
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
                  "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson",
                  "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "King", "Wright"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Generate a tweet
def generate_tweet(company, sentiment, date):
    # Replace placeholders in phrases
    if sentiment == "positive":
        base_text = random.choice(company["positive_phrases"])
    else:
        base_text = random.choice(company["negative_phrases"])
    
    # Replace placeholders
    product = random.choice(company["topics"])
    text = base_text.replace("{product}", product).replace("{company}", company["name"])
    
    # Add hashtags (30% chance)
    hashtags_string = ""
    hashtags_list = []
    if random.random() < 0.3:
        hashtags_list = random.sample(company["hashtags"], k=random.randint(1, 3))
        hashtags_string = " ".join([f"#{tag}" for tag in hashtags_list])
        text += " " + hashtags_string
    
    # Generate sentiment score
    if sentiment == "positive":
        score = round(random.uniform(0.7, 0.95), 2)
        confidence = round(random.uniform(0.85, 0.98), 2)
    else:
        score = round(random.uniform(0.05, 0.3), 2)
        confidence = round(random.uniform(0.8, 0.95), 2)
    
    # Generate user data
    username = generate_username()
    name = generate_name()
    profile_image = random.choice(profile_images)
    followers = random.randint(100, 10000)
    
    # Generate metrics
    if sentiment == "positive":
        like_base = random.randint(50, 300)
    else:
        like_base = random.randint(30, 200)
    
    retweet_count = int(like_base * random.uniform(0.1, 0.5))
    reply_count = int(like_base * random.uniform(0.05, 0.3))
    quote_count = int(like_base * random.uniform(0.02, 0.1))
    
    # Create tweet object
    tweet = {
        "id": str(uuid.uuid4()),
        "text": text,
        "created_at": date.isoformat(),
        "company": company["name"],
        "sentiment": {
            "score": score,
            "label": sentiment,
            "confidence": confidence
        },
        "user": {
            "username": username,
            "name": name,
            "profile_image_url": profile_image,
            "followers_count": followers
        },
        "metrics": {
            "retweet_count": retweet_count,
            "reply_count": reply_count,
            "like_count": like_base,
            "quote_count": quote_count
        },
        "hashtags": hashtags_string
    }
    
    # For backward compatibility, keep entities but we'll ignore it in the database
    if hashtags_list:
        tweet["entities"] = {
            "hashtags": hashtags_list
        }
    
    return tweet

# Generate tweets for all companies
def generate_all_tweets(num_tweets_per_company=5000, days=365):
    all_tweets = []
    now = datetime.now()
    
    for company in companies:
        print(f"Generating tweets for {company['name']}...")
        
        # Generate positive tweets (65%)
        positive_count = int(num_tweets_per_company * 0.65)
        for _ in range(positive_count):
            days_ago = random.randint(0, days)
            tweet_date = now - timedelta(days=days_ago, 
                                        hours=random.randint(0, 23),
                                        minutes=random.randint(0, 59),
                                        seconds=random.randint(0, 59))
            all_tweets.append(generate_tweet(company, "positive", tweet_date))
        
        # Generate negative tweets (35%)
        negative_count = num_tweets_per_company - positive_count
        for _ in range(negative_count):
            days_ago = random.randint(0, days)
            tweet_date = now - timedelta(days=days_ago,
                                        hours=random.randint(0, 23),
                                        minutes=random.randint(0, 59),
                                        seconds=random.randint(0, 59))
            all_tweets.append(generate_tweet(company, "negative", tweet_date))
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(all_tweets)
    
    # Add created_at as datetime for sorting
    df['datetime'] = pd.to_datetime(df['created_at'])
    
    # Sort by date
    df = df.sort_values('datetime', ascending=False)
    
    # Drop the datetime column used for sorting
    df = df.drop('datetime', axis=1)
    
    return df

# Main execution
if __name__ == "__main__":
    print("Generating mock tweet data...")
    tweets_df = generate_all_tweets(num_tweets_per_company=5000)
    
    # Save to CSV file for inspection (optional)
    tweets_df.to_csv('mock_tweets.csv', index=False)
    
    # Also save to JSON for compatibility with existing code
    tweets_df.to_json('mock_tweets.json', orient='records', indent=2)
    
    print(f"Done! Generated {len(tweets_df)} tweets.")
    print(f"Files created: mock_tweets.csv and mock_tweets.json")
