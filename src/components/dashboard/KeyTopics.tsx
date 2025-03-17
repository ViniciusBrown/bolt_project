import React from 'react';
import { CompanySentiment } from '@/types';

interface KeyTopicsProps {
  data: CompanySentiment;
}

export function KeyTopics({ data }: KeyTopicsProps) {
  const getSentimentColor = (score: number) => {
    if (score >= 0.7) return 'bg-green-500';
    if (score >= 0.5) return 'bg-green-400';
    if (score >= 0.4) return 'bg-yellow-500';
    if (score >= 0.3) return 'bg-orange-500';
    return 'bg-red-500';
  };

  return (
    <div className="space-y-4">
      {data.key_topics.length > 0 ? (
        data.key_topics.map((topic, index) => (
          <div key={index} className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <div className={`w-2 h-2 rounded-full ${getSentimentColor(topic.sentiment_score)}`} />
              <span className="font-medium">{topic.topic}</span>
            </div>
            <div className="flex items-center space-x-4">
              <span className="text-sm text-muted-foreground">{topic.count.toLocaleString()}</span>
              <div className="w-16 bg-muted rounded-full h-2 overflow-hidden">
                <div 
                  className={`h-full ${getSentimentColor(topic.sentiment_score)}`} 
                  style={{ width: `${topic.sentiment_score * 100}%` }}
                />
              </div>
              <span className="text-sm">{topic.sentiment_score.toFixed(2)}</span>
            </div>
          </div>
        ))
      ) : (
        <div className="text-center py-4 text-muted-foreground">
          No topics available for this time period
        </div>
      )}
    </div>
  );
}
