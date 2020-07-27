package us.writable;


import java.util.Calendar;

public class ISODateWritable extends DateWritable {
    ISODateWritable() {/*Default constructor for serialization. Do not instantiate.*/}

    // UTC time when tweet was created from twitter api's created_at json field
    public ISODateWritable(String isoDate) {
        this.date = isoDate;
    }

    @Override
    Calendar parseDateStringToCalendar() {
        // Format: YYYYMMDD
        int year = Integer.parseInt(date.substring(0, 4));
        // Calendar months are zero based.
        int month = Integer.parseInt(date.substring(4, 6)) - 1;
        int day = Integer.parseInt(date.substring(6));
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        return calendar;
    }
}
