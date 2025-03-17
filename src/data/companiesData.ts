import { CompanySentiment } from '@/types';

export const companiesData: CompanySentiment[] = [
  {
    company: "Apple Inc.",
    logo_url: "https://images.unsplash.com/photo-1611186871348-b1ce696e52c9?q=80&w=100&auto=format&fit=crop",
    time_period: "Last 30 days",
    sentiment_summary: {
      overall_score: 0.78,
      positive_percentage: 65,
      negative_percentage: 15,
      neutral_percentage: 20,
      total_tweets: 12500
    },
    sentiment_trend: [
      { date: "2023-05-01", average_score: 0.72, tweet_count: 350 },
      { date: "2023-05-08", average_score: 0.75, tweet_count: 420 },
      { date: "2023-05-15", average_score: 0.79, tweet_count: 380 },
      { date: "2023-05-22", average_score: 0.81, tweet_count: 450 },
      { date: "2023-05-29", average_score: 0.78, tweet_count: 410 }
    ],
    top_tweets: {
      positive: [
        {
          id: "1",
          text: "Just got my new iPhone 14 Pro and I'm absolutely loving it! The camera quality is insane! #Apple #iPhone14Pro",
          created_at: "2023-05-28T14:23:10Z",
          sentiment: { score: 0.92, label: "positive", confidence: 0.95 },
          user: {
            username: "techfan42",
            name: "Tech Enthusiast",
            profile_image_url: "https://images.unsplash.com/photo-1535713875002-d1d0cf377fde?q=80&w=100&auto=format&fit=crop",
            followers_count: 1250
          },
          metrics: {
            retweet_count: 45,
            reply_count: 12,
            like_count: 189,
            quote_count: 5
          },
          entities: {
            hashtags: ["Apple", "iPhone14Pro"]
          }
        },
        {
          id: "2",
          text: "Apple's customer service is top notch. Had an issue with my MacBook and they resolved it immediately. This is why I stay loyal to the brand!",
          created_at: "2023-05-25T09:45:22Z",
          sentiment: { score: 0.88, label: "positive", confidence: 0.91 },
          user: {
            username: "sarahsmith",
            name: "Sarah Smith",
            profile_image_url: "https://images.unsplash.com/photo-1494790108377-be9c29b29330?q=80&w=100&auto=format&fit=crop",
            followers_count: 3400
          },
          metrics: {
            retweet_count: 23,
            reply_count: 8,
            like_count: 156,
            quote_count: 2
          }
        }
      ],
      negative: [
        {
          id: "3",
          text: "Disappointed with the battery life on my new MacBook Pro. Expected better from Apple at this price point. #AppleFail",
          created_at: "2023-05-27T16:12:45Z",
          sentiment: { score: 0.25, label: "negative", confidence: 0.87 },
          user: {
            username: "techreviewerguy",
            name: "Tech Reviewer",
            profile_image_url: "https://images.unsplash.com/photo-1570295999919-56ceb5ecca61?q=80&w=100&auto=format&fit=crop",
            followers_count: 8900
          },
          metrics: {
            retweet_count: 78,
            reply_count: 45,
            like_count: 210,
            quote_count: 12
          },
          entities: {
            hashtags: ["AppleFail"]
          }
        }
      ]
    },
    key_topics: [
      { topic: "iPhone 14", count: 3250, sentiment_score: 0.82 },
      { topic: "iOS 16", count: 2100, sentiment_score: 0.75 },
      { topic: "MacBook Pro", count: 1850, sentiment_score: 0.68 },
      { topic: "Apple Watch", count: 1200, sentiment_score: 0.79 },
      { topic: "AirPods", count: 950, sentiment_score: 0.81 }
    ]
  },
  {
    company: "Tesla, Inc.",
    logo_url: "https://images.unsplash.com/photo-1617704548623-340376564e68?q=80&w=100&auto=format&fit=crop",
    time_period: "Last 30 days",
    sentiment_summary: {
      overall_score: 0.62,
      positive_percentage: 52,
      negative_percentage: 28,
      neutral_percentage: 20,
      total_tweets: 18700
    },
    sentiment_trend: [
      { date: "2023-05-01", average_score: 0.58, tweet_count: 520 },
      { date: "2023-05-08", average_score: 0.61, tweet_count: 580 },
      { date: "2023-05-15", average_score: 0.65, tweet_count: 610 },
      { date: "2023-05-22", average_score: 0.63, tweet_count: 590 },
      { date: "2023-05-29", average_score: 0.62, tweet_count: 600 }
    ],
    top_tweets: {
      positive: [
        {
          id: "4",
          text: "My Tesla Model 3 just got the new software update and the improvements are incredible! The autopilot is smoother than ever. #Tesla #EV",
          created_at: "2023-05-26T11:34:18Z",
          sentiment: { score: 0.89, label: "positive", confidence: 0.93 },
          user: {
            username: "evlover",
            name: "Electric Vehicle Enthusiast",
            profile_image_url: "https://images.unsplash.com/photo-1599566150163-29194dcaad36?q=80&w=100&auto=format&fit=crop",
            followers_count: 2300
          },
          metrics: {
            retweet_count: 67,
            reply_count: 23,
            like_count: 245,
            quote_count: 8
          },
          entities: {
            hashtags: ["Tesla", "EV"]
          }
        }
      ],
      negative: [
        {
          id: "5",
          text: "Tesla service centers are overwhelmed. Been waiting 3 weeks for an appointment for a simple issue. This needs to be addressed @elonmusk",
          created_at: "2023-05-24T15:22:40Z",
          sentiment: { score: 0.28, label: "negative", confidence: 0.85 },
          user: {
            username: "teslaowner2020",
            name: "Model Y Owner",
            profile_image_url: "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?q=80&w=100&auto=format&fit=crop",
            followers_count: 780
          },
          metrics: {
            retweet_count: 112,
            reply_count: 89,
            like_count: 320,
            quote_count: 15
          },
          entities: {
            mentions: ["elonmusk"]
          }
        }
      ]
    },
    key_topics: [
      { topic: "Model Y", count: 4200, sentiment_score: 0.71 },
      { topic: "Full Self-Driving", count: 3800, sentiment_score: 0.58 },
      { topic: "Elon Musk", count: 3500, sentiment_score: 0.52 },
      { topic: "Cybertruck", count: 2900, sentiment_score: 0.68 },
      { topic: "Supercharger", count: 1800, sentiment_score: 0.75 }
    ]
  },
  {
    company: "Microsoft",
    logo_url: "https://images.unsplash.com/photo-1583339793403-3d9b001b6008?q=80&w=100&auto=format&fit=crop",
    time_period: "Last 30 days",
    sentiment_summary: {
      overall_score: 0.71,
      positive_percentage: 58,
      negative_percentage: 22,
      neutral_percentage: 20,
      total_tweets: 14200
    },
    sentiment_trend: [
      { date: "2023-05-01", average_score: 0.68, tweet_count: 420 },
      { date: "2023-05-08", average_score: 0.70, tweet_count: 450 },
      { date: "2023-05-15", average_score: 0.72, tweet_count: 480 },
      { date: "2023-05-22", average_score: 0.73, tweet_count: 460 },
      { date: "2023-05-29", average_score: 0.71, tweet_count: 470 }
    ],
    top_tweets: {
      positive: [
        {
          id: "6",
          text: "Windows 11 is actually a great improvement over Windows 10. The UI is cleaner and performance is noticeably better. Good job @Microsoft!",
          created_at: "2023-05-27T10:15:30Z",
          sentiment: { score: 0.85, label: "positive", confidence: 0.90 },
          user: {
            username: "techguru",
            name: "Tech Guru",
            profile_image_url: "https://images.unsplash.com/photo-1568602471122-7832951cc4c5?q=80&w=100&auto=format&fit=crop",
            followers_count: 5600
          },
          metrics: {
            retweet_count: 54,
            reply_count: 18,
            like_count: 210,
            quote_count: 7
          },
          entities: {
            mentions: ["Microsoft"]
          }
        }
      ],
      negative: [
        {
          id: "7",
          text: "Microsoft Teams keeps crashing during important meetings. This is unacceptable for enterprise software that we pay so much for! #MicrosoftFail",
          created_at: "2023-05-25T14:45:12Z",
          sentiment: { score: 0.22, label: "negative", confidence: 0.88 },
          user: {
            username: "enterpriseIT",
            name: "Enterprise IT Manager",
            profile_image_url: "https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?q=80&w=100&auto=format&fit=crop",
            followers_count: 3200
          },
          metrics: {
            retweet_count: 87,
            reply_count: 65,
            like_count: 290,
            quote_count: 14
          },
          entities: {
            hashtags: ["MicrosoftFail"]
          }
        }
      ]
    },
    key_topics: [
      { topic: "Windows 11", count: 3800, sentiment_score: 0.74 },
      { topic: "Microsoft Teams", count: 3200, sentiment_score: 0.62 },
      { topic: "Xbox", count: 2700, sentiment_score: 0.78 },
      { topic: "Office 365", count: 2400, sentiment_score: 0.68 },
      { topic: "Azure", count: 2100, sentiment_score: 0.75 }
    ]
  }
];
