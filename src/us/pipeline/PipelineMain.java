package us.pipeline;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PipelineMain {
    static final String INPUT_FILE_PATH = "covid/data/sample/sample.jsonl";
    static final String OUTPUT_FILE_PATH = "covid/out/sample";

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(JsonToTweetMapper.class);
        job.setJobName("Json to Tweet Mapper");
        FileInputFormat.addInputPath(job, new Path(INPUT_FILE_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE_PATH));

        job.setMapperClass(JsonToTweetMapper.class);
        job.setOutputKeyClass(CreatedAtWritable.class);
        job.setOutputValueClass(TweetWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
