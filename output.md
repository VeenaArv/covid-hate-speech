# Output

## Data Schema

See `schema/annotated_tweet_schema.jsonl`

## Impala Commands

### To tell impala to read from parquet file format:

```
CREATE EXTERNAL TABLE twitter_data LIKE PARQUET 'covid/parquet/tweets/out/*'
  STORED AS PARQUET
  LOCATION 'covid/parquet/tweets/out';
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

