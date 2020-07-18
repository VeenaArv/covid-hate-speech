package java;

public class PipelineCounters {
    enum Preprocessing {
        JSON_PARSE_EXCEPTION,
        NUM_TWEETS_PROCESSED,
        NUM_OUTPUTS_JSON_TO_TWEET_MAPPER,
        NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS,
        NUM_DUPLICATE_TWEETS
    }
}
