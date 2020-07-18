package java;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.utils.PipelineUtils;
import java.writable.CreatedAtWritable;
import java.writable.TweetWritable;

public class RemoveDuplicatesReducer extends Reducer<TweetWritable, TweetWritable, CreatedAtWritable, TweetWritable> {
    @Override
    protected void reduce(TweetWritable key, Iterable<TweetWritable> values, Context context)
            throws IOException, InterruptedException {
        CreatedAtWritable createdAt = new CreatedAtWritable(key.createdAt);
        context.getCounter(PipelineCounters.Preprocessing.NUM_DUPLICATE_TWEETS).increment(size(values) - 1);
        context.getCounter(PipelineCounters.Preprocessing.NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS).increment(1);
        context.write(createdAt, key);
    }

    private int size(Iterable<TweetWritable> values) {
        int size = 0;
        for (TweetWritable tweet : values) {
            size++;
        }
        return size;
    }
}