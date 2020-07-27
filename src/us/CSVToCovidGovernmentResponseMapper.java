package us;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import us.utils.PipelineUtils;
import us.writable.CovidGovernmentResponseWritable;
import us.writable.ISODateWritable;
import us.writable.TweetWritable;

import java.io.IOException;

public class CSVToCovidGovernmentResponseMapper  extends Mapper<LongWritable, Text, ISODateWritable, CovidGovernmentResponseWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PipelineUtils.parseGovernmentResponsefromCSV(value.toString());
    }
}
