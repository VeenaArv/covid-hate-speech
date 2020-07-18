# Impala Commands

## Input Data Schema

| Column Name       | Type          |
| ----------------- |:-------------:|
| created_at        | TIMESTAMP     |
| raw_text          | string        |
| preprocessed_text | string        |
| sentiment_score   | integer       |
| sentiment_prob    | float         |

```
SELECT *,
       avg(sentiment_prob) as avg_sentiment_prob,
       sum(sentiment_score) as total_sentiment,
       count(*) as num_tweets
FROM
   {table_name} GROUP BY created_at, order by created_at;
```

## Final Data Schema
| Column Name       | Type          |
| ----------------- |:-------------:|
| created_at        | TIMESTAMP     |
| raw_text          | string        |
| preprocessed_text | string        |
| sentiment_score   | integer       |
| sentiment_prob    | float         |
| avg_probability   | string        |
| total_sentiment   | integer       |
| num_tweets        | integer       |