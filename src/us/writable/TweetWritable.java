package us.writable;

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
    public String userLocation;
    public String countryCode;
    public String createdAt;
    public Boolean isTruncated;
    // Calculations.
    public boolean isEligibleForAnalysis;
    public String preprocessedText;
    public String state;


    public TweetWritable(Long tweetId, String rawText, String hashtags, String userLocation, String countryCode, String createdAt, Boolean isTruncated) {
        this.tweetId = tweetId;
        this.rawText = rawText;
        this.hashtags = hashtags;
        this.userLocation = userLocation;
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
        out.writeUTF(userLocation);
        out.writeUTF(countryCode);
        out.writeUTF(createdAt);
        out.writeBoolean(isTruncated);
        out.writeBoolean(isEligibleForAnalysis);
        out.writeUTF(preprocessedText);
        out.writeUTF(state);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tweetId = in.readLong();
        rawText = in.readUTF();
        hashtags = in.readUTF();
        userLocation = in.readUTF();
        countryCode = in.readUTF();
        createdAt = in.readUTF();
        isTruncated = in.readBoolean();
        isEligibleForAnalysis = in.readBoolean();
        preprocessedText = in.readUTF();
        state = in.readUTF();
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
        return tweetId != null && rawText != null && hashtags != null && createdAt != null
                && isTruncated != null && !isTruncated && isLocatedInUS();
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

    /**
     * Naive regex based check for US user locations.
     * @return true if located in the United States.
     */
    private boolean isLocatedInUS() {
        // Geotagged location is more accurate than user provided location.
        if (countryCode != null) {
            return countryCode.equals("US");
        }
        if (userLocation == null) {
            return false;
        }
        if (userLocation.contains("USA") || userLocation.contains("United States")) {
            return true;
        }

        // Location that contains states
        String[] STATES = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
                "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana",
                "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
                "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
                "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
                "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
                "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"};
        String[] STATES_ABBREV = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL",
                "IN", "IA", "IL", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
                "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
                "WA", "WV", "WI", "WY"};
        for (int i = 0; i < STATES.length; i++) {
            if (userLocation.contains(STATES[i])) {
                state = STATES[i];
                return true;
            }
            // supports city, state or city,state or city state.
            // Assumes city is a real city in that state.
            // TODO(veenaarv) use maps api to find location from string.
            String regex = ".+[, ]" + STATES_ABBREV[i] + "$";
            if (userLocation.trim().matches(regex)) {
                state = STATES[i];
                return true;
            }
        }
        return false;
    }

}