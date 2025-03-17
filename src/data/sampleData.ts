import { CompanySentiment } from '../types';
import { subDays, format } from 'date-fns';

// Generate dates within the last month
const today = new Date();
const generateRecentDate = (daysAgo: number) => {
  return format(subDays(today, daysAgo), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'');
};

export const sampleData: CompanySentiment = {
  "company": "Tesla",
  "logo_url": "https://logo.clearbit.com/tesla.com",
  "time_period": "Last 30 days",
  "sentiment_summary": {
    "overall_score": 0.65,
    "positive_percentage": 58,
    "negative_percentage": 22,
    "neutral_percentage": 20,
    "total_tweets": 12500
  },
  "sentiment_trend": [
    {
      "date": format(subDays(today, 29), 'yyyy-MM-dd'),
      "average_score": 0.62,
      "tweet_count": 1750
    },
    {
      "date": format(subDays(today, 25), 'yyyy-MM-dd'),
      "average_score": 0.58,
      "tweet_count": 1680
    },
    {
      "date": format(subDays(today, 21), 'yyyy-MM-dd'),
      "average_score": 0.61,
      "tweet_count": 1520
    },
    {
      "date": format(subDays(today, 17), 'yyyy-MM-dd'),
      "average_score": 0.67,
      "tweet_count": 1890
    },
    {
      "date": format(subDays(today, 13), 'yyyy-MM-dd'),
      "average_score": 0.71,
      "tweet_count": 2100
    },
    {
      "date": format(subDays(today, 9), 'yyyy-MM-dd'),
      "average_score": 0.68,
      "tweet_count": 1950
    },
    {
      "date": format(subDays(today, 5), 'yyyy-MM-dd'),
      "average_score": 0.69,
      "tweet_count": 1610
    },
    {
      "date": format(subDays(today, 1), 'yyyy-MM-dd'),
      "average_score": 0.72,
      "tweet_count": 2000
    }
  ],
  "top_tweets": {
    "positive": [
      // Month tweets (21-30 days old)
      {
        "id": "1401234567890",
        "text": "Just got my new Tesla Model Y and I'm absolutely blown away by the performance and technology! Best car purchase ever! #Tesla #ModelY #ElectricVehicles",
        "created_at": generateRecentDate(28),
        "sentiment": {
          "score": 0.92,
          "label": "positive",
          "confidence": 0.98
        },
        "user": {
          "username": "techEnthusiast",
          "name": "Tech Enthusiast",
          "profile_image_url": "https://images.unsplash.com/photo-1535713875002-d1d0cf377fde?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=100&q=80",
          "followers_count": 3452
        },
        "metrics": {
          "retweet_count": 128,
          "reply_count": 32,
          "like_count": 457,
          "quote_count": 12
        },
        "entities": {
          "hashtags": ["Tesla", "ModelY", "ElectricVehicles"]
        }
      },
      // Week tweets (4-7 days old)
      {
        "id": "1401298765432",
        "text": "Tesla's Autopilot just saved me from a potential accident on the highway. The safety features are incredible and worth every penny. Thank you @elonmusk for prioritizing safety!",
        "created_at": generateRecentDate(6),
        "sentiment": {
          "score": 0.89,
          "label": "positive",
          "confidence": 0.95
        },
        "user": {
          "username": "safetyFirst",
          "name": "Safety Advocate",
          "profile_image_url": "https://images.unsplash.com/photo-1494790108377-be9c29b29330?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=100&q=80",
          "followers_count": 1876
        },
        "metrics": {
          "retweet_count": 215,
          "reply_count": 54,
          "like_count": 789,
          "quote_count": 23
        },
        "entities": {
          "mentions": ["elonmusk"]
        }
      },
      // Day tweets (0-1 days old)
      {
        "id": "1401387654321",
        "text": "Tesla's commitment to sustainable energy is changing the world. Just installed Solar Roof and Powerwall - my house is now completely energy independent! Amazing technology and customer service throughout the process.",
        "created_at": generateRecentDate(1),
        "sentiment": {
          "score": 0.87,
          "label": "positive",
          "confidence": 0.93
        },
        "user": {
          "username": "greenLiving",
          "name": "Sustainable Future",
          "profile_image_url": "https://images.unsplash.com/photo-1539571696357-5a69c17a67c6?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=100&q=80",
          "followers_count": 5621
        },
        "metrics": {
          "retweet_count": 342,
          "reply_count": 87,
          "like_count": 1256,
          "quote_count": 45
        }
      }
    ],
    "negative": [
      // Month tweets (21-30 days old)
      {
        "id": "1401123456789",
        "text": "Third service appointment this month for my Model 3. Quality control issues are getting ridiculous. Starting to regret my purchase. @Tesla customer service needs serious improvement.",
        "created_at": generateRecentDate(25),
        "sentiment": {
          "score": 0.12,
          "label": "negative",
          "confidence": 0.91
        },
        "user": {
          "username": "disappointedOwner",
          "name": "Disappointed Customer",
          "profile_image_url": "https://images.unsplash.com/photo-1500648767791-00dcc994a43e?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=100&q=80",
          "followers_count": 892
        },
        "metrics": {
          "retweet_count": 76,
          "reply_count": 104,
          "like_count": 321,
          "quote_count": 18
        },
        "entities": {
          "mentions": ["Tesla"]
        }
      },
      // Week tweets (4-7 days old)
      {
        "id": "1401187654321",
        "text": "Price increases AGAIN? This is getting absurd. Tesla promised affordable electric vehicles for everyone but keeps pushing prices higher. Feeling misled as a long-time supporter. #Tesla #PriceHike",
        "created_at": generateRecentDate(5),
        "sentiment": {
          "score": 0.18,
          "label": "negative",
          "confidence": 0.88
        },
        "user": {
          "username": "valueShopper",
          "name": "Budget Conscious",
          "profile_image_url": "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=100&q=80",
          "followers_count": 1243
        },
        "metrics": {
          "retweet_count": 189,
          "reply_count": 67,
          "like_count": 542,
          "quote_count": 31
        },
        "entities": {
          "hashtags": ["Tesla", "PriceHike"]
        }
      },
      // Day tweets (0-1 days old)
      {
        "id": "1401276543210",
        "text": "Autopilot almost caused an accident today. Suddenly swerved for no reason. This technology is not ready for public roads and Tesla should be more transparent about its limitations. Scary experience.",
        "created_at": generateRecentDate(1),
        "sentiment": {
          "score": 0.15,
          "label": "negative",
          "confidence": 0.90
        },
        "user": {
          "username": "safetyExpert",
          "name": "Road Safety Advocate",
          "profile_image_url": "https://images.unsplash.com/photo-1506794778202-cad84cf45f1d?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=100&q=80",
          "followers_count": 4231
        },
        "metrics": {
          "retweet_count": 267,
          "reply_count": 92,
          "like_count": 631,
          "quote_count": 47
        }
      }
    ]
  },
  "key_topics": [
    {
      "topic": "Model Y",
      "count": 2340,
      "sentiment_score": 0.78
    },
    {
      "topic": "Autopilot",
      "count": 1870,
      "sentiment_score": 0.62
    },
    {
      "topic": "Elon Musk",
      "count": 1650,
      "sentiment_score": 0.59
    },
    {
      "topic": "Service",
      "count": 1420,
      "sentiment_score": 0.41
    },
    {
      "topic": "Price",
      "count": 1280,
      "sentiment_score": 0.38
    },
    {
      "topic": "Solar",
      "count": 980,
      "sentiment_score": 0.81
    },
    {
      "topic": "Cybertruck",
      "count": 840,
      "sentiment_score": 0.74
    },
    {
      "topic": "Supercharger",
      "count": 720,
      "sentiment_score": 0.69
    }
  ]
};
