package us.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnnotatedTweetWritable implements WritableComparable<AnnotatedTweetWritable> {
    // Thresholds to filer on for hate speech detection.
    public static double MIN_PROBABILITY = 0.7;
    public static SentimentClass MIN_SENTIMENT_CLASS = SentimentClass.NEGATIVE;

    public TweetWritable tweet;
    // Obtained from CoreNLP SentimentAnnotator.
    public int avgSentimentScore;
    public double avgSentimentProbability;
    // Strategy to determine sentiment by using the most negative sentiment. Filter sentences with neutral sentiment
    // may inflate value
    public int minSentimentScore;
    public double minSentimentProbability;

    public AnnotatedTweetWritable(){/*Default constructor for serialization. Do not instantiate.*/}

    public AnnotatedTweetWritable(TweetWritable tweet) {
        this.tweet = tweet;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tweet.write(out);
        out.write(avgSentimentScore);
        out.writeDouble(avgSentimentProbability);
        out.write(minSentimentScore);
        out.writeDouble(minSentimentProbability);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tweet.readFields(in);
        avgSentimentScore = in.readInt();
        avgSentimentProbability = in.readDouble();
        minSentimentScore = in.readInt();
        minSentimentProbability = in.readDouble();
    }

    @Override
    public int compareTo(AnnotatedTweetWritable o) {
        return this.confidenceScore().compareTo(o.confidenceScore());
    }

    public Double confidenceScore() {
        // With 0 being neutral.
        int normalizedSentimentScore = avgSentimentScore - 2;
        return avgSentimentProbability * normalizedSentimentScore * -1;
    }

    public static SentimentClass toSentimentClass(int sentimentScore) {
        switch (sentimentScore) {
            case 0:
                return SentimentClass.VERY_NEGATIVE;
            case 1:
                return SentimentClass.NEGATIVE;
            case 2:
                return SentimentClass.NEUTRAL;
            case 3:
                return SentimentClass.POSITIVE;
            default:
                break;
        }
        return SentimentClass.UNKNOWN;
    }

    enum SentimentClass {
        VERY_NEGATIVE,
        NEGATIVE,
        NEUTRAL,
        POSITIVE,
        UNKNOWN
    }

}
