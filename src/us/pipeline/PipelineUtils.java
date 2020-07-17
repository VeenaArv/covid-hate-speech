package us.pipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;

class CreatedAtWritable implements WritableComparable<CreatedAtWritable> {
    // UTC time when tweet was created from twitter api's created_at json field
    String createdAt;

    CreatedAtWritable() {/*Default constructor for serialization. Do not instantiate.*/}

    CreatedAtWritable(String createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Parses created_at json field as a date.
     *
     * @return The createdAt field parse as a calendar object with time discarded.
     */
    private Calendar parseTwitterDateStringToCalendar() {
        ArrayList<String> MONTH_CODE_TO_INT = new ArrayList<>(
                Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"));
        // ex. "created_at": "Wed Oct 10 20:19:24 +0000 2018"
        String[] splitDate = createdAt.split(" ");
        int year = Integer.parseInt(splitDate[splitDate.length - 1]);
        int month = MONTH_CODE_TO_INT.indexOf(splitDate[1]) + 1;
        int day = Integer.parseInt(splitDate[2]);
        Calendar calendar = Calendar.getInstance();
        // TODO(veenaarv) fix month desired parameter syntax issue
        calendar.set(year, month, day);
        return calendar;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(createdAt);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        createdAt = in.readLine();
    }

    @Override
    public int compareTo(CreatedAtWritable o) {
        return this.parseTwitterDateStringToCalendar().compareTo(o.parseTwitterDateStringToCalendar());
    }
}

class TweetWritable implements Writable {
    public static final Log log = LogFactory.getLog(TweetWritable.class);
    // Attributes from twitter API.
    Long tweetId;
    String raw_text;
    String hashtags;
    String countryCode;
    String createdAt;
    Boolean isTruncated;
    // Calculations.
    boolean isEligibleForAnalysis;
    String preprocessedText;

    public TweetWritable(Long tweetId, String raw_text, String hashtags, String countryCode, String createdAt, Boolean isTruncated) {
        this.tweetId = tweetId;
        this.raw_text = raw_text;
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
        out.writeChars(raw_text);
        out.writeChars(hashtags);
        out.writeChars(countryCode);
        out.writeChars(createdAt);
        out.writeBoolean(isTruncated);
        out.writeBoolean(isEligibleForAnalysis);
        out.writeChars(preprocessedText);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tweetId = in.readLong();
        raw_text = in.readLine();
        hashtags = in.readLine();
        countryCode = in.readLine();
        createdAt = in.readLine();
        isTruncated = in.readBoolean();
        isEligibleForAnalysis = in.readBoolean();
        preprocessedText = in.readLine();
    }

    /**
     * Sets `isEligibleForAnalysis` field that filters tweets that are eligible for sentiment analysis.
     */
    private boolean isEligibleForAnalysis() {
        return tweetId != null && raw_text != null && hashtags != null && countryCode != null && createdAt != null
                && isTruncated != null && !isTruncated && countryCode.equals("US");
    }

    /**
     * Preprocesses text for sentiment analysis and hate speech detection. Steps include making characters all
     * lowercase, removing punctuation, replacing all whitespaces with a space, and removing stop words.
     */
    private String preprocessTextForNLP() {
        return removeStopwords(raw_text.toLowerCase()
                .replaceAll("\\p{Punct}", "")
                .replaceAll("\\s+", " "));
    }

}

public class PipelineUtils {
    /**
     * @param jsonString representing a single tweet
     * @return parsed String as a tweet
     * @throws ParseException
     */
    public static TweetWritable parseJSONObjectFromString(String jsonString) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(jsonString);
        // System.out.println(jsonObject.toJSONString());
        JSONObject entitiesJsonObject = (JSONObject) jsonObject.get("entities");
        JSONObject placeJsonObject = (JSONObject) jsonObject.get("place");
        String hashtags = entitiesJsonObject == null || entitiesJsonObject.get("hashtags") == null
                ? null : entitiesJsonObject.get("hashtags").toString();
        String place = placeJsonObject == null ? null : placeJsonObject.get("country_code").toString();
        return new TweetWritable((Long) jsonObject.get("id"), (String) jsonObject.get("full_text"), hashtags, place,
                (String) jsonObject.get("created_at"), (Boolean) jsonObject.get("truncated"));
    }

    public static void main(String[] args) throws FileNotFoundException, ParseException {
        Scanner sc = new Scanner(new File("data/sample.jsonl"));
        String jsonString = sc.nextLine();
        TweetWritable tweet = parseJSONObjectFromString(jsonString);
        System.out.println(tweet.raw_text);

    }

    enum Counters {
        JSON_PARSE_EXCEPTION,
        NUM_TWEETS_PROCESSED,
        NUM_OUTPUTS_JSON_TO_TWEET_MAPPER,
        NUM_TWEETS_ELIGIBLE_FOR_ANALYSIS,
        NUM_DUPLICATE_TWEETS
    }
}