package us;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;
import us.writable.AnnotatedTweetWritable;
import us.writable.CreatedAtWritable;
import us.writable.TweetWritable;

import java.io.File;
import java.io.IOException;


public class AnnotatedTweetToParquetMapper extends Mapper<CreatedAtWritable, AnnotatedTweetWritable, Void, GenericRecord> {

    public static void main(String[] args) throws Exception {
        Schema MAPPING_SCHEMA = new Schema.Parser().parse(new File("annotated_tweet_schema.jsonl"));
        CreatedAtWritable key = new CreatedAtWritable("Wed Oct 10 20:19:24 +0000 2018");
        AnnotatedTweetWritable value = new AnnotatedTweetWritable(new TweetWritable(10l, "this is end", "", "USA", "", "", false));
        value.minSentimentProbability = .5;
        value.minSentimentScore = 2;
        value.avgSentimentProbability = .75;
        value.avgSentimentScore = 3;
        GenericData.Record record = new GenericData.Record(MAPPING_SCHEMA);
        record.put("created_date", key.toISODate());
        record.put("raw_text", value.tweet.rawText);
        record.put("user_location", value.tweet.userLocation);
        record.put("us_state", value.tweet.state);
        record.put("avg_sentiment_score", value.avgSentimentScore);
        record.put("avg_sentiment_class", AnnotatedTweetWritable.toSentimentClass(value.avgSentimentScore));
        record.put("avg_sentiment_prob", value.avgSentimentProbability);
        record.put("min_sentiment_score", value.minSentimentScore);
        record.put("min_sentiment_class", AnnotatedTweetWritable.toSentimentClass(value.minSentimentScore));
        record.put("min_sentiment_prob", value.minSentimentProbability);
        System.out.println(record.getSchema().toString(true));
    }

    @Override
    protected void map(CreatedAtWritable key, AnnotatedTweetWritable value, Context context) throws IOException, InterruptedException {
        Schema schema = new Schema.Parser().parse(new File("annotated_tweet_schema.jsonl"));
        GenericData.Record record = new GenericData.Record(schema);
        if (value.tweet != null) {
            record.put("created_date", key.toISODate());
            record.put("raw_text", value.tweet.rawText);
            record.put("user_location", value.tweet.userLocation);
            record.put("us_state", value.tweet.state);
            record.put("avg_sentiment_score", value.avgSentimentScore);
            record.put("avg_sentiment_class", AnnotatedTweetWritable.toSentimentClass(value.avgSentimentScore));
            record.put("avg_sentiment_prob", value.avgSentimentProbability);
            record.put("min_sentiment_score", value.minSentimentScore);
            record.put("min_sentiment_class", AnnotatedTweetWritable.toSentimentClass(value.minSentimentScore));
            record.put("min_sentiment_prob", value.minSentimentProbability);
            context.getCounter(PipelineCounters.writeToParquet.NUM_TWEETS_PROCESSED).increment(1);
            context.write(null, record);
        }
    }
}
