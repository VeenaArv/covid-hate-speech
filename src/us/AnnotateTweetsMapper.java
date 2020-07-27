package us;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import us.utils.NLPUtils;
import us.writable.AnnotatedTweetWritable;
import us.writable.CreatedAtWritable;
import us.writable.TweetWritable;

public class AnnotateTweetsMapper
        extends Mapper<CreatedAtWritable, TweetWritable, CreatedAtWritable, AnnotatedTweetWritable> {
    @Override
    protected void map(CreatedAtWritable key, TweetWritable value, Context context)
            throws IOException, InterruptedException {
        AnnotatedTweetWritable annotatedTweetWritable = new AnnotatedTweetWritable(value);
        NLPUtils.getSentiment(annotatedTweetWritable);
        context.write(key, annotatedTweetWritable);
    }
}
