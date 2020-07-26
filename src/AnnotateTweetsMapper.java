import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import utils.NLPUtils;
import writable.AnnotatedTweetWritable;
import writable.CreatedAtWritable;
import writable.TweetWritable;

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
