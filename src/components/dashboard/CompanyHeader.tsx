import React from 'react';
import { CompanySentiment } from '@/types';
import { Calendar } from 'lucide-react';

interface CompanyHeaderProps {
  data: CompanySentiment;
}

export function CompanyHeader({ data }: CompanyHeaderProps) {
  return (
    <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
      <div className="flex items-center gap-4">
        <div className="h-12 w-12 rounded-full overflow-hidden">
          <img 
            src={data.logo_url} 
            alt={data.company} 
            className="h-full w-full object-cover"
          />
        </div>
        <div>
          <h2 className="text-2xl font-bold">{data.company}</h2>
          <div className="flex items-center text-sm text-muted-foreground">
            <Calendar className="mr-1 h-4 w-4" />
            <span>{data.time_period}</span>
          </div>
        </div>
      </div>
      
      <div className="flex items-center gap-4">
        <div className="flex flex-col items-end">
          <div className="flex items-center">
            <span className="text-sm font-medium mr-2">Overall Sentiment:</span>
            <span className={`font-bold ${getSentimentColor(data.sentiment_summary.overall_score)}`}>
              {data.sentiment_summary.overall_score.toFixed(2)}
            </span>
          </div>
          <div className="text-sm text-muted-foreground">
            Based on {data.sentiment_summary.total_tweets.toLocaleString()} tweets
          </div>
        </div>
      </div>
    </div>
  );
}

function getSentimentColor(score: number) {
  if (score >= 0.7) return 'text-green-500';
  if (score >= 0.5) return 'text-green-400';
  if (score >= 0.4) return 'text-yellow-500';
  if (score >= 0.3) return 'text-orange-500';
  return 'text-red-500';
}
