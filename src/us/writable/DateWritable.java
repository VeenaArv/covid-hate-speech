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
    public abstract Calendar toCalendar();

    /**
     * Coverts to int readable by avro date logical type
     * A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch,
     * 1 January 1970 (ISO calendar).
     */
    public abstract long toAvroDate();

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
        return this.toCalendar().compareTo(o.toCalendar());
    }
}
