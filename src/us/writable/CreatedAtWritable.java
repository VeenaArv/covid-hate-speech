package us.writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;

public class CreatedAtWritable extends DateWritable {

    CreatedAtWritable() {/*Default constructor for serialization. Do not instantiate.*/}

    // UTC time when tweet was created from twitter api's created_at json field
    public CreatedAtWritable(String createdAt) {
        this.date = createdAt;
    }

    /**
     * Parses created_at json field as a date.
     * @return The createdAt field parse as a calendar object with time discarded.
     */
    @Override
    Calendar parseDateStringToCalendar() {
        ArrayList<String> MONTH_CODE_TO_INT = new ArrayList<>(
                Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"));
        // ex. "created_at": "Wed Oct 10 20:19:24 +0000 2018"
        String[] splitDate = date.split(" ");
        int year = Integer.parseInt(splitDate[splitDate.length - 1]);
        // Java months are zero based.
        int month = MONTH_CODE_TO_INT.indexOf(splitDate[1]);
        int day = Integer.parseInt(splitDate[2]);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        return calendar;
    }
}