import { useState } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card, CardContent } from '@/components/ui/card';
import { CompanyHeader } from '@/components/dashboard/CompanyHeader';
import { SentimentSummary } from '@/components/dashboard/SentimentSummary';
import { SentimentTrend } from '@/components/dashboard/SentimentTrend';
import { TopTweets } from '@/components/dashboard/TopTweets';
import { KeyTopics } from '@/components/dashboard/KeyTopics';
import { CompanySearch } from '@/components/dashboard/CompanySearch';
import { companiesData } from '@/data/companiesData';
import { Separator } from '@/components/ui/separator';
import { parseISO, differenceInDays, subMonths, subYears } from 'date-fns';
import { Tweet } from '@/types';
import { UserMenu } from '@/components/auth/UserMenu';
import { AuthDialog } from '@/components/auth/AuthDialog';
import { useAuth } from '@/context/AuthContext';

function App() {
  const [selectedCompany, setSelectedCompany] = useState(companiesData[0]);
  const [timeFilter, setTimeFilter] = useState("month");
  const [isAuthDialogOpen, setIsAuthDialogOpen] = useState(false);
  const { isAuthenticated } = useAuth();

  // Filter tweets based on time period
  const filterTweetsByDate = (tweets: Tweet[], maxDaysOld: number): Tweet[] => {
    const today = new Date();
    return tweets.filter(tweet => {
      const tweetDate = parseISO(tweet.created_at);
      const daysDifference = differenceInDays(today, tweetDate);
      return daysDifference <= maxDaysOld;
    });
  };

  // Filter data based on selected time period
  const getFilteredData = () => {
    // Define max days old for each time period
    const maxDays = {
      year: 365,
      sixMonths: 180,
      month: 30,
      week: 7,
      day: 1
    };

    // For longer time periods, we'll use the available tweets but adjust their dates
    // In a real app, we would fetch historical data from an API
    const getAdjustedTweets = (tweets: Tweet[], period: string): Tweet[] => {
      if (period === 'month' || period === 'week' || period === 'day') {
        return filterTweetsByDate(tweets, maxDays[period as keyof typeof maxDays]);
      }
      
      // For longer periods, use all tweets but adjust their dates to spread across the period
      return tweets.map((tweet, index) => {
        const totalTweets = tweets.length;
        const daysSpan = period === 'sixMonths' ? 180 : 365;
        const daysAgo = Math.floor((daysSpan / totalTweets) * (totalTweets - index - 1));
        
        return {
          ...tweet,
          created_at: period === 'sixMonths' 
            ? subMonths(new Date(), 6 - (index * 6 / totalTweets)).toISOString()
            : subMonths(new Date(), 12 - (index * 12 / totalTweets)).toISOString()
        };
      });
    };

    // Filter tweets based on time period
    const filteredPositiveTweets = getAdjustedTweets(
      selectedCompany.top_tweets.positive, 
      timeFilter
    );
    
    const filteredNegativeTweets = getAdjustedTweets(
      selectedCompany.top_tweets.negative, 
      timeFilter
    );

    // Generate trend data based on time period
    const generateTrendData = () => {
      const baseTrend = selectedCompany.sentiment_trend;
      
      if (timeFilter === 'day') return baseTrend.slice(-1);
      if (timeFilter === 'week') return baseTrend.slice(-7);
      if (timeFilter === 'month') return baseTrend;
      
      // For longer periods, generate additional data points
      const today = new Date();
      const dataPoints = timeFilter === 'sixMonths' ? 24 : 48; // Bi-weekly for 6 months, monthly for 1 year
      const result = [];
      
      for (let i = 0; i < dataPoints; i++) {
        const pointDate = timeFilter === 'sixMonths'
          ? subMonths(today, 6 * (1 - i/dataPoints))
          : subMonths(today, 12 * (1 - i/dataPoints));
          
        // Create some variation in the data
        const randomFactor = 0.9 + Math.random() * 0.2; // 0.9 to 1.1
        const baseIndex = i % baseTrend.length;
        
        result.push({
          date: pointDate.toISOString().split('T')[0],
          average_score: Math.min(1, Math.max(0, baseTrend[baseIndex].average_score * randomFactor)),
          tweet_count: Math.floor(baseTrend[baseIndex].tweet_count * (0.8 + Math.random() * 0.4))
        });
      }
      
      return result;
    };

    // Generate trend data
    const filteredTrend = generateTrendData();

    // Adjust topic data based on time period
    const getAdjustedTopics = () => {
      let multiplier = 1;
      let scoreAdjustment = 1;
      let topicLimit = selectedCompany.key_topics.length;
      
      switch (timeFilter) {
        case 'day':
          multiplier = 0.15;
          scoreAdjustment = 0.9;
          topicLimit = 5;
          break;
        case 'week':
          multiplier = 0.4;
          scoreAdjustment = 1.1;
          topicLimit = 6;
          break;
        case 'month':
          multiplier = 1;
          scoreAdjustment = 1;
          break;
        case 'sixMonths':
          multiplier = 4.5;
          scoreAdjustment = 0.95;
          break;
        case 'year':
          multiplier = 8;
          scoreAdjustment = 0.9;
          break;
      }
      
      const adjusted = selectedCompany.key_topics.map(topic => ({
        ...topic,
        count: Math.floor(topic.count * multiplier),
        sentiment_score: Math.max(0, Math.min(1, topic.sentiment_score * scoreAdjustment))
      }));
      
      return adjusted.slice(0, topicLimit);
    };

    // Adjust sentiment summary based on time period
    const getAdjustedSummary = () => {
      const base = selectedCompany.sentiment_summary;
      
      switch (timeFilter) {
        case 'day':
          return {
            ...base,
            overall_score: 0.58,
            positive_percentage: 48,
            negative_percentage: 32,
            neutral_percentage: 20,
            total_tweets: 850
          };
        case 'week':
          return {
            ...base,
            overall_score: 0.65,
            positive_percentage: 55,
            negative_percentage: 20,
            neutral_percentage: 25,
            total_tweets: 3500
          };
        case 'month':
          return base;
        case 'sixMonths':
          return {
            ...base,
            overall_score: 0.61,
            positive_percentage: 52,
            negative_percentage: 28,
            neutral_percentage: 20,
            total_tweets: 42000
          };
        case 'year':
          return {
            ...base,
            overall_score: 0.59,
            positive_percentage: 50,
            negative_percentage: 30,
            neutral_percentage: 20,
            total_tweets: 85000
          };
      }
      
      return base;
    };

    // Get time period label
    const getTimePeriodLabel = () => {
      switch (timeFilter) {
        case 'day': return 'Last 24 hours';
        case 'week': return 'Last 7 days';
        case 'month': return 'Last 30 days';
        case 'sixMonths': return 'Last 6 months';
        case 'year': return 'Last 12 months';
        default: return 'Last 30 days';
      }
    };

    return {
      ...selectedCompany,
      time_period: getTimePeriodLabel(),
      sentiment_summary: getAdjustedSummary(),
      sentiment_trend: filteredTrend,
      key_topics: getAdjustedTopics(),
      top_tweets: {
        positive: filteredPositiveTweets,
        negative: filteredNegativeTweets
      }
    };
  };

  const filteredData = getFilteredData();

  return (
    <div className="flex min-h-screen flex-col">
      <header className="sticky top-0 z-10 border-b bg-background/95 backdrop-blur">
        <div className="container flex h-16 items-center justify-between py-4">
          <div className="flex items-center gap-2">
            <h1 className="text-xl font-bold">Twitter Sentiment Dashboard</h1>
          </div>
          <div className="flex items-center gap-4">
            <div className="w-[280px]">
              <CompanySearch 
                companies={companiesData} 
                onSelectCompany={setSelectedCompany} 
                currentCompany={selectedCompany}
              />
            </div>
            <UserMenu onLoginClick={() => setIsAuthDialogOpen(true)} />
          </div>
        </div>
      </header>
      <main className="flex-1 container py-6">
        <div className="space-y-6">
          <CompanyHeader data={filteredData} />
          
          <Tabs 
            defaultValue="month" 
            className="space-y-6"
            value={timeFilter}
            onValueChange={setTimeFilter}
          >
            <div className="flex justify-between items-center">
              <h2 className="text-2xl font-bold">Dashboard</h2>
              <TabsList>
                <TabsTrigger value="year">Year</TabsTrigger>
                <TabsTrigger value="sixMonths">6 Months</TabsTrigger>
                <TabsTrigger value="month">Month</TabsTrigger>
                <TabsTrigger value="week">Week</TabsTrigger>
                <TabsTrigger value="day">Day</TabsTrigger>
              </TabsList>
            </div>
            
            <div className="space-y-6">
              {/* Sentiment Summary Section */}
              <SentimentSummary data={filteredData} />
              
              {/* Sentiment Trend Section */}
              <SentimentTrend data={filteredData} />
              
              {/* Two Column Layout for Topics and Tweets */}
              <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
                {/* Key Topics Section */}
                <Card className="lg:col-span-4">
                  <CardContent className="p-6">
                    <h3 className="text-lg font-medium mb-4">Key Topics</h3>
                    <KeyTopics data={filteredData} />
                  </CardContent>
                </Card>
                
                {/* Tweets Section */}
                <Card className="lg:col-span-8">
                  <CardContent className="p-6">
                    <div className="space-y-6">
                      <div>
                        <h3 className="text-lg font-medium">Top Positive Tweets</h3>
                        <Separator className="my-2" />
                        <TopTweets tweets={filteredData.top_tweets.positive} />
                      </div>
                      
                      <div>
                        <h3 className="text-lg font-medium">Top Negative Tweets</h3>
                        <Separator className="my-2" />
                        <TopTweets tweets={filteredData.top_tweets.negative} />
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
          </Tabs>
        </div>
      </main>
      
      {/* Authentication Dialog */}
      <AuthDialog 
        isOpen={isAuthDialogOpen} 
        onClose={() => setIsAuthDialogOpen(false)} 
      />
    </div>
  );
}

export default App;
