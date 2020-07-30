package us.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
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

    public AnnotatedTweetWritable() {/*Default constructor for serialization. Do not instantiate.*/}

    public AnnotatedTweetWritable(TweetWritable tweet) {
        this.tweet = tweet;
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
            case 4:
                return SentimentClass.VERY_POSITIVE;
            default:
                break;
        }
        return SentimentClass.UNKNOWN;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tweet.tweetId);
        out.writeUTF(tweet.rawText);
        out.writeUTF(tweet.hashtags);
        out.writeUTF(tweet.userLocation);
        out.writeUTF(tweet.countryCode);
        out.writeUTF(tweet.createdAt);
        out.writeBoolean(tweet.isTruncated);
        out.writeBoolean(tweet.isEligibleForAnalysis);
        out.writeUTF(tweet.preprocessedText);
        out.writeUTF(tweet.state);
        out.writeInt(avgSentimentScore);
        out.writeDouble(avgSentimentProbability);
        out.writeInt(minSentimentScore);
        out.writeDouble(minSentimentProbability);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            long tweetId = in.readLong();
            String rawText = in.readUTF();
            String hashtags = in.readUTF();
            String userLocation = in.readUTF();
            String countryCode = in.readUTF();
            String createdAt = in.readUTF();
            boolean isTruncated = in.readBoolean();
            boolean isEligibleForAnalysis = in.readBoolean();
            String preprocessedText = in.readUTF();
            String state = in.readUTF();
            avgSentimentScore = in.readInt();
            avgSentimentProbability = in.readDouble();
            minSentimentScore = in.readInt();
            minSentimentProbability = in.readDouble();
            tweet = new TweetWritable(tweetId, rawText, hashtags, userLocation, countryCode, createdAt, isTruncated);
        } catch (EOFException e) {
        }
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

    enum SentimentClass {
        VERY_NEGATIVE,
        NEGATIVE,
        NEUTRAL,
        POSITIVE,
        VERY_POSITIVE,
        UNKNOWN
    }

}
