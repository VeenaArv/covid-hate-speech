package us.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

public abstract class DateWritable implements WritableComparable<DateWritable> {
    String date;

    /**
     * Creates a calendar object to compare dates.
     */
    abstract Calendar parseDateStringToCalendar();

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date = in.readLine();
    }

    @Override
    public int compareTo(DateWritable o) {
        return this.parseDateStringToCalendar().compareTo(o.parseDateStringToCalendar());
    }
}
