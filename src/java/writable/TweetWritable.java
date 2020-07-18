package java.writable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class TweetWritable implements WritableComparable<TweetWritable> {
    public static final Log log = LogFactory.getLog(TweetWritable.class);
    // Attributes from twitter API.
    public Long tweetId;
    public String rawText;
    public String hashtags;
    public String countryCode;
    public String createdAt;
    public Boolean isTruncated;
    // Calculations.
    public boolean isEligibleForAnalysis;
    public String preprocessedText;


    public TweetWritable(Long tweetId, String rawText, String hashtags, String countryCode, String createdAt, Boolean isTruncated) {
        this.tweetId = tweetId;
        this.rawText = rawText;
        this.hashtags = hashtags;
        this.countryCode = countryCode;
        this.createdAt = createdAt;
        this.isTruncated = isTruncated;
        isEligibleForAnalysis = isEligibleForAnalysis();
        preprocessedText = preprocessTextForNLP();
    }

    TweetWritable() {/*Default constructor for serialization. Do not instantiate.*/}

    /**
     * @param text must be lowercased and stripped of punctutation and extra whitespace.
     * @return text with stopwords removed.
     */
    private static String removeStopwords(String text) {
        String[] STOPWORDS_ARR = {"a", "an", "and", "are", "as", "at", "be", "but", "by",
                "for", "if", "in", "into", "is", "it",
                "no", "not", "of", "on", "or", "such",
                "that", "the", "their", "then", "there", "these",
                "they", "this", "to", "was", "will", "with"};

        HashSet<String> stopwords = new HashSet<>(Arrays.asList(STOPWORDS_ARR));
        StringBuilder stringBuilder = new StringBuilder();
        for (String word : text.split(" ")) {
            if (!stopwords.contains(word)) {
                stringBuilder.append(word).append(" ");
            }
        }
        // removes trailing whitespace.
        return stringBuilder.toString().trim();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tweetId);
        out.writeUTF(rawText);
        out.writeUTF(hashtags);
        out.writeUTF(countryCode);
        out.writeUTF(createdAt);
        out.writeBoolean(isTruncated);
        out.writeBoolean(isEligibleForAnalysis);
        out.writeUTF(preprocessedText);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tweetId = in.readLong();
        rawText = in.readUTF();
        hashtags = in.readUTF();
        countryCode = in.readUTF();
        createdAt = in.readUTF();
        isTruncated = in.readBoolean();
        isEligibleForAnalysis = in.readBoolean();
        preprocessedText = in.readUTF();
    }

    @Override
    // Only used to find duplicates. Sorting by tweetId has no meaningful value.
    public int compareTo(TweetWritable o) {
        return this.tweetId.compareTo(o.tweetId);
    }

    /**
     * Sets `isEligibleForAnalysis` field that filters tweets that are eligible for sentiment analysis.
     */
    private boolean isEligibleForAnalysis() {
        return tweetId != null && rawText != null && hashtags != null && countryCode != null && createdAt != null
                && isTruncated != null && !isTruncated && countryCode.equals("US");
    }

    /**
     * Preprocesses text for sentiment analysis and hate speech detection. Steps include making characters all
     * lowercase, removing punctuation, replacing all whitespaces with a space, and removing stop words.
     */
    private String preprocessTextForNLP() {
        return removeStopwords(rawText.toLowerCase()
                .replaceAll("\\p{Punct}", "")
                .replaceAll("\\s+", " "));
    }

}