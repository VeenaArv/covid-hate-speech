package java.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;
import java.writable.TweetWritable;


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

    }

}