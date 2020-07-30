package us.writable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

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
    public Calendar toCalendar() {
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

    /**
     * Parses created_at json field as a date.
     * @return The createdAt field parse as a calendar object with time discarded.
     */
    @Override
    public long toAvroDate() {
        ArrayList<String> MONTH_CODE_TO_INT = new ArrayList<>(
                Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"));
        // ex. "created_at": "Wed Oct 10 20:19:24 +0000 2018"
        String[] splitDate = date.split(" ");
        int year = Integer.parseInt(splitDate[splitDate.length - 1]);
        // From 1 (January) to 12 (December)
        int month = MONTH_CODE_TO_INT.indexOf(splitDate[1]) + 1;
        int day = Integer.parseInt(splitDate[2]);

        LocalDate epoch = LocalDate.ofEpochDay(0);
        LocalDate date = LocalDate.of(year, month, day);
        return ChronoUnit.DAYS.between(epoch, date);
    }

    public String toISODate() {
        ArrayList<String> MONTH_CODE_TO_INT = new ArrayList<>(
                Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"));
        // ex. "created_at": "Wed Oct 10 20:19:24 +0000 2018"
        String[] splitDate = date.split(" ");
        int year = Integer.parseInt(splitDate[splitDate.length - 1]);
        // From 1 (January) to 12 (December)
        int month = MONTH_CODE_TO_INT.indexOf(splitDate[1]);
        int day = Integer.parseInt(splitDate[2]);

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);
        String iso = df.format(new Date(year, month, day));
        return iso;
    }

    public String getMonth() {
        return date.split(" ")[1];
    }
}