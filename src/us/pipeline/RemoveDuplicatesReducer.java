package us.pipeline;

import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RemoveDuplicatesReducer extends Reducer<TweetWritable, TweetWritable, CreatedAtWritable, TweetWritable> {
    @Override
    protected void reduce(TweetWritable key, Iterable<TweetWritable> values, Context context)
            throws IOException, InterruptedException {
        CreatedAtWritable createdAt = new CreatedAtWritable(key.createdAt);
        context.getCounter(PipelineUtils.Counters.NUM_DUPLICATE_TWEETS).increment(IterableUtils.size(values));
        context.getCounter(PipelineUtils.Counters.NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS).increment(1);
        context.write(createdAt, key);
    }
}
