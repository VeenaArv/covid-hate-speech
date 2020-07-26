import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.ParseException;
import utils.PipelineUtils;
import writable.TweetWritable;

import java.io.IOException;

public class JsonToTweetMapper extends Mapper<LongWritable, Text, TweetWritable, TweetWritable> {
    public static final Log log = LogFactory.getLog(JsonToTweetMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            TweetWritable tweet = PipelineUtils.parseJSONTweetObjectFromString(value.toString());
            context.getCounter(PipelineCounters.TweetPipeline.NUM_TWEETS_PROCESSED).increment(1);
            if (tweet.isEligibleForAnalysis) {
                context.write(tweet, tweet);
                context.getCounter(PipelineCounters.TweetPipeline.NUM_OUTPUTS_JSON_TO_TWEET_MAPPER).increment(1);
            }

        } catch (ParseException e) {
            context.getCounter(PipelineCounters.TweetPipeline.JSON_PARSE_EXCEPTION).increment(1);
        }
    }
}
