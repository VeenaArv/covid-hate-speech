package us;

import org.apache.hadoop.mapreduce.Mapper;
import us.utils.NLPUtils;
import us.writable.AnnotatedTweetWritable;
import us.writable.CreatedAtWritable;
import us.writable.TweetWritable;

import java.io.IOException;


public class AnnotateTweetsMapper
        extends Mapper<CreatedAtWritable, TweetWritable, CreatedAtWritable, AnnotatedTweetWritable> {
    @Override
    protected void map(CreatedAtWritable key, TweetWritable value, Context context)
            throws IOException, InterruptedException {
        AnnotatedTweetWritable annotatedTweetWritable = new AnnotatedTweetWritable(value);
        NLPUtils.populateSentiment(annotatedTweetWritable, context);
        incrementCounters(annotatedTweetWritable, context);
        // A negative sentiment indicates that the model was unable to classify sentiment of one or more sentences.
        if (annotatedTweetWritable.minSentimentProbability >= 0) {
            context.write(key, annotatedTweetWritable);
            context.getCounter(PipelineCounters.annotateTweets.NUM_OUTPUTS_ANNOTATE_TWEETS_MAPPER).increment(1);
        }
    }

    private void incrementCounters(AnnotatedTweetWritable annotatedTweetWritable, Context context) {
        switch (annotatedTweetWritable.avgSentimentScore) {
            case 0:
                context.getCounter(PipelineCounters.annotateTweets.NUM_AVG_VERY_NEGATIVE).increment(1);
                break;
            case 1:
                context.getCounter(PipelineCounters.annotateTweets.NUM_AVG_NEGATIVE).increment(1);
                break;
            case 2:
                context.getCounter(PipelineCounters.annotateTweets.NUM_AVG_NEUTRAL).increment(1);
                break;
            case 3:
                context.getCounter(PipelineCounters.annotateTweets.NUM_AVG_POSITIVE).increment(1);
                break;
            case 4:
                context.getCounter(PipelineCounters.annotateTweets.NUM_AVG_VERY_POSITIVE).increment(1);
                break;
            default:
                context.getCounter(PipelineCounters.annotateTweets.NUM_AVG_UNKNOWN).increment(1);
        }
        switch (annotatedTweetWritable.minSentimentScore) {
            case 0:
                context.getCounter(PipelineCounters.annotateTweets.NUM_MIN_VERY_NEGATIVE).increment(1);
                break;
            case 1:
                context.getCounter(PipelineCounters.annotateTweets.NUM_MIN_NEGATIVE).increment(1);
                break;
            case 2:
                context.getCounter(PipelineCounters.annotateTweets.NUM_MIN_NEUTRAL).increment(1);
                break;
            case 3:
                context.getCounter(PipelineCounters.annotateTweets.NUM_MIN_POSITIVE).increment(1);
                break;
            case 4:
                context.getCounter(PipelineCounters.annotateTweets.NUM_MIN_VERY_POSITIVE).increment(1);
                break;
            default:
                context.getCounter(PipelineCounters.annotateTweets.NUM_MIN_UNKNOWN).increment(1);
        }


    }
}
