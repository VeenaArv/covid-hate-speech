package java;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.utils.NLPUtils;
import java.writable.AnnotatedTweetWritable;
import java.writable.CreatedAtWritable;
import java.writable.TweetWritable;

public class AnnotateTweetsMapper
        extends Mapper<CreatedAtWritable, TweetWritable, CreatedAtWritable, AnnotatedTweetWritable> {
    @Override
    protected void map(CreatedAtWritable key, TweetWritable value, Context context)
            throws IOException, InterruptedException {
        AnnotatedTweetWritable annotatedTweetWritable = new AnnotatedTweetWritable(value);
        NLPUtils.getSentiment(value.rawText, annotatedTweetWritable);
        context.write(key, annotatedTweetWritable);
    }
}
