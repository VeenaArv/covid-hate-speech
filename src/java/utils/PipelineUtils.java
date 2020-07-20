package java.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.writable.CovidGovernmentResponseWritable;
import java.writable.PoliciesWritable;
import java.writable.TweetWritable;


public class PipelineUtils {

    /**
     * @param jsonString representing a single tweet
     * @return parsed String as a tweet
     * @throws ParseException
     */
    public static TweetWritable parseJSONTweetObjectFromString(String jsonString) throws ParseException {
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

    private static int parseDecimalStringAsInt(String decimal) {
        return (int) Double.parseDouble(decimal);
    }

    public static CovidGovernmentResponseWritable parseGovernmentResponsefromCSV(String line) {
        // CountryName,CountryCode,Date,C1_School closing,C1_Flag,C2_Workplace closing,C2_Flag,C3_Cancel public events,
        // C3_Flag,C4_Restrictions on gatherings,C4_Flag,C5_Close public transport,C5_Flag,C6_Stay at home requirements,
        // C6_Flag,C7_Restrictions on internal movement,C7_Flag,C8_International travel controls,E1_Income support,
        // E1_Flag,E2_Debt/contract relief,E3_Fiscal measures,E4_International support,H1_Public information campaigns,
        // H1_Flag,H2_Testing policy,H3_Contact tracing,H4_Emergency investment in healthcare,H5_Investment in vaccines,
        // M1_Wildcard,ConfirmedCases,ConfirmedDeaths,StringencyIndex,StringencyIndexForDisplay,StringencyLegacyIndex,
        // StringencyLegacyIndexForDisplay,GovernmentResponseIndex,GovernmentResponseIndexForDisplay,
        // ContainmentHealthIndex,ContainmentHealthIndexForDisplay,EconomicSupportIndex,EconomicSupportIndexForDisplay
        String[] columns = line.split(",");
        PoliciesWritable policies = new PoliciesWritable()
                .setSchoolClosing(parseDecimalStringAsInt(columns[3]))
                .setWorkPlaceClosing(parseDecimalStringAsInt(columns[5]))
                .setCancelPublicEvents(parseDecimalStringAsInt(columns[7]))
                .setRestrictionsOnGatherings(parseDecimalStringAsInt(columns[9]))
                .setClosePublicTransport(parseDecimalStringAsInt(columns[11]))
                .setStayAtHomeRequirments(parseDecimalStringAsInt(columns[13]))
                .setRestrictionsOnInternalMovement(parseDecimalStringAsInt(columns[15]))
                .setInternationalTravelControls(parseDecimalStringAsInt(columns[17]))
                .setIncomeSupport(parseDecimalStringAsInt(columns[18]))
                .setDebtRelief(parseDecimalStringAsInt(columns[20]))
                .setFiscalMeasures(parseDecimalStringAsInt(columns[21]))
                .setInternationalSupport(parseDecimalStringAsInt(columns[22]))
                .setPublicInfoCampaigns(parseDecimalStringAsInt(columns[23]))
                .setTestingPolicy(parseDecimalStringAsInt(columns[25]))
                .setContactTracing(parseDecimalStringAsInt(columns[26]))
                .setEmergencyInvestmentInHealthcare(parseDecimalStringAsInt(columns[27]))
                .setInvestmentInVaccines(parseDecimalStringAsInt(columns[28]))
                .setWildardPolicies(columns[29]).build();
        return
                new CovidGovernmentResponseWritable()
                        .setCountryCode(columns[1])
                        .setDate(columns[2])
                        .setConfirmedCases(Integer.parseInt(columns[30]))
                        .setDeaths(Integer.parseInt(columns[31]))
                        .setStringencyActual(Double.parseDouble(columns[32]))
                        .setStringency(Double.parseDouble(columns[33]))
                        .setStringencyLegacy(Double.parseDouble(columns[34]))
                        .setGovernmentResponseIndex(Double.parseDouble(columns[35]))
                        .setContainmentHealthIndex(Double.parseDouble(columns[37]))
                        .setEconomicSupportIndex(Double.parseDouble(columns[39]))
                        .build();


    }


    public static void main(String[] args) throws FileNotFoundException, ParseException {
//        Scanner sc = new Scanner(new File("data/sample.jsonl"));
//        String jsonString = sc.nextLine();
//        TweetWritable tweet = parseJSONObjectFromString(jsonString);
        String line1 = "United States,USA,20200712,,,,,,,,,,,,,,,,,,,,,,,,,,,,3247684,134814,,,,,,,,,,";
        String line2 = "United States,USA,20200713,,,,,,,,,,,,,,,,,,,,,,,,,,,,3304942,135205,,68.98,,68.57,,68.91,,70.08,,62.50";
        String line3 = "United States,USA,20200706,3.00,0,2.00,0,2.00,0,4.00,0,1.00,0,2.00,0,2.00,0,3.00,2.00,0,1.00,0.00,0.00,2.00,1,3.00,1.00,0.00,0.00,,2888635,129947,68.98,68.98,68.57,68.57,68.91,68.91,70.08,70.08,62.50,62.50";
    }

}