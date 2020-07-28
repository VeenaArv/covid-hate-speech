# Output

## Data Schema

See `schema/annotated_tweet_schema.jsonl`

## Impala Commands

### To tell impala to read from parquet file format:

```
CREATE EXTERNAL TABLE twitter_data LIKE PARQUET 'covid/parquet/tweets/out/*'
  STORED AS PARQUET
  LOCATION 'covid/parquet/tweets/out';

NUM_DUPLICATE_TWEETS	0	22	22
NUM_OUTPUTS_JSON_TO_TWEET_MAPPER	296443	0	296443
NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS	0	296421	296421
NUM_TWEETS_PROCESSED	1974940	0	1974940
```

### Sentiment scores grouped by day

```
SELECT created_date,
       raw_text,
       if(user_location='NULL', 'US', user_location) as location,
       avg(avg_sentiment_prob) as avg_sentiment_prob_avg_strategy,
       sum(avg_sentiment_score) as total_sentiment_avg_strategy,
       avg(min_sentiment_prob) as avg_sentiment_prob_min_strategy,
       sum(min_sentiment_score) as total_sentiment_min_strategy,
       count(*) as num_tweets
FROM
   twitter_data GROUP BY created_date, order by created_date;
```

