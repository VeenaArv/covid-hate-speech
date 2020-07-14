package us.pipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class JsonToTweetMapper extends Mapper<LongWritable, Text, CreatedAtWritable, TweetWritable> {
    public static final Log log = LogFactory.getLog(JsonToTweetMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            TweetWritable tweet = PipelineUtils.parseJSONObjectFromString(value.toString());
            CreatedAtWritable createdAt = new CreatedAtWritable(tweet.createdAt);
            context.getCounter(PipelineUtils.Counters.NUM_TWEETS_PROCESSED).increment(1);
            if (tweet.isEligibleForAnalysis) {
                context.write(createdAt, tweet);
                context.getCounter(PipelineUtils.Counters.NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS).increment(1);
            }

        } catch (ParseException e) {
            context.getCounter(PipelineUtils.Counters.JSON_PARSE_EXCEPTION).increment(1);
        }
    }
}
