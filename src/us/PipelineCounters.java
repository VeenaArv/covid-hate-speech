package us;

public class PipelineCounters {
    public enum extractTweets {
        JSON_PARSE_EXCEPTION,
        NUM_TWEETS_PROCESSED,
        NUM_OUTPUTS_JSON_TO_TWEET_MAPPER,
        NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS,
        NUM_DUPLICATE_TWEETS
    }

    public enum annotateTweets {
        NUM_AVG_VERY_NEGATIVE,
        NUM_AVG_NEGATIVE,
        NUM_AVG_NEUTRAL,
        NUM_AVG_POSITIVE,
        NUM_AVG_VERY_POSITIVE,
        NUM_AVG_UNKNOWN,
        NUM_MIN_VERY_NEGATIVE,
        NUM_MIN_NEGATIVE,
        NUM_MIN_NEUTRAL,
        NUM_MIN_POSITIVE,
        NUM_MIN_VERY_POSITIVE,
        NUM_MIN_UNKNOWN,
        NUM_ZERO_PROBABILITY,
        NUM_OUTPUTS_ANNOTATE_TWEETS_MAPPER
    }

    public enum writeToParquet {
        NUM_TWEETS_PROCESSED
    }
}
