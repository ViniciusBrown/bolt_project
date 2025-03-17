export interface Tweet {
  id: string;
  text: string;
  created_at: string;
  sentiment: {
    score: number;
    label: string;
    confidence: number;
  };
  user: {
    username: string;
    name: string;
    profile_image_url: string;
    followers_count: number;
  };
  metrics: {
    retweet_count: number;
    reply_count: number;
    like_count: number;
    quote_count: number;
  };
  entities?: {
    hashtags: string[];
  };
}

export interface SentimentSummary {
  overall_score: number;
  positive_percentage: number;
  negative_percentage: number;
  neutral_percentage: number;
  total_tweets: number;
}

export interface SentimentTrendPoint {
  date: string;
  average_score: number;
  tweet_count: number;
}

export interface Topic {
  topic: string;
  count: number;
  sentiment_score: number;
}

export interface CompanySentiment {
  company: string;
  logo_url: string;
  time_period: string;
  sentiment_summary: SentimentSummary;
  sentiment_trend: SentimentTrendPoint[];
  top_tweets: {
    positive: Tweet[];
    negative: Tweet[];
  };
  key_topics: Topic[];
}

export interface User {
  id: string;
  email: string;
  name: string;
  avatarUrl?: string;
}

export interface AuthState {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
}
