package us.pipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Scanner;

class Tweet {
    public static final Log log = LogFactory.getLog(Tweet.class);
    Long tweetId;
    String text;
    String hashtags;
    String countryCode;
    Calendar createdAt;
    Boolean isTrucated;
    boolean isValidForProcessing;

    public Tweet(Long tweetId, String text, String hashtags, String countryCode, String createdAt, Boolean isTruncated) {
        this.tweetId = tweetId;
        this.text = text;
        this.hashtags = hashtags;
        this.countryCode = countryCode;
        this.createdAt = parseTwitterDateStringToCalendar(createdAt);

        this.isTrucated = isTruncated;
        isValidForProcessing = isValidForProcessing();
    }

    /**
     * Parses created_at json field as a date.
     *
     * @param createdAt UTC time when tweet was created from twitter api's created_at json field
     * @return The createdAt field parse as a calendar object with time discarded.
     */
    private static Calendar parseTwitterDateStringToCalendar(String createdAt) {
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

    /**
     * Sets `isValidForProcessing` field that filters tweets that are eligible for sentiment analysis.
     */
    private boolean isValidForProcessing() {
        return tweetId != null && text != null && countryCode != null && !isTrucated && countryCode.equals("US");
    }
}


public class PipelineUtils {
    /**
     * @param jsonString representing a single tweet
     * @return parsed String as a tweet
     * @throws ParseException
     */
    public static Tweet parseJSONObjectFromString(String jsonString) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(jsonString);
        // System.out.println(jsonObject.toJSONString());
        JSONObject entitiesJsonObject = (JSONObject) jsonObject.get("entities");
        JSONObject placeJsonObject = (JSONObject) jsonObject.get("place");
        // TODO(veenaarv) parse hashtags as an array
        String hashtags = entitiesJsonObject == null || entitiesJsonObject.get("hashtags") == null ? null : entitiesJsonObject.get("hashtags").toString();
        String place = placeJsonObject == null ? null : placeJsonObject.get("country_code").toString();
        return new Tweet((Long) jsonObject.get("id"), (String) jsonObject.get("full_text"), hashtags, place,
                (String) jsonObject.get("created_at"), (Boolean) jsonObject.get("truncated"));


    }

    public static void main(String[] args) throws FileNotFoundException, ParseException {
        Scanner sc = new Scanner(new File("data/sample.jsonl"));
        String jsonString = sc.nextLine();
        Tweet tweet = parseJSONObjectFromString(jsonString);
        System.out.println(tweet.text);

    }
}

