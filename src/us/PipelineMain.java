package us;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;
import us.writable.AnnotatedTweetWritable;
import us.writable.CreatedAtWritable;
import us.writable.TweetWritable;
import java.io.File;

public class PipelineMain {
    static final String INPUT_TWEETS_FILE_PATH = "covid/data/coronavirus-through-09-June-2020-00.jsonl";
    static final String OUTPUT_TWEETS_FILE_PATH = "covid/out/";
    static final String INPUT_ANNOTATION_FILE_PATH = OUTPUT_TWEETS_FILE_PATH;
    static final String OUTPUT_ANNOTATION_FILE_PATH = "covid/annotation/out2";
    static final String OUTPUT_PARQUET_FILE_PATH = "covid/parquet/tweets/out";


    public static void main(String[] args) throws Exception {
        if (!runExtractTweets()) System.exit(1);
        if (!runAnnotateTweets()) System.exit(1);
        if (!runWriteTweetsToParquetFile()) System.exit(1);
        System.exit(0);
    }

        public static boolean runExtractTweets() throws Exception {
        Job extractTweets = Job.getInstance();
        extractTweets.setJarByClass(JsonToTweetMapper.class);
        extractTweets.setJobName("Json to Tweet Mapper");

        extractTweets.setMapperClass(JsonToTweetMapper.class);
        extractTweets.setReducerClass(RemoveDuplicatesReducer.class);

        extractTweets.setMapOutputKeyClass(TweetWritable.class);
        extractTweets.setMapOutputValueClass(TweetWritable.class);
        extractTweets.setOutputKeyClass(CreatedAtWritable.class);
        extractTweets.setOutputValueClass(TweetWritable.class);

        FileInputFormat.addInputPath(extractTweets, new Path(INPUT_TWEETS_FILE_PATH));
        FileOutputFormat.setOutputPath(extractTweets, new Path(OUTPUT_TWEETS_FILE_PATH));

        extractTweets.setOutputFormatClass(SequenceFileOutputFormat.class);

        return extractTweets.waitForCompletion(true);
    }

    public static boolean runAnnotateTweets() throws Exception {
        Job annotateTweets = Job.getInstance();
        annotateTweets.addFileToClassPath(new Path("stanford-corenlp-4.0.0.jar"));
        annotateTweets.addFileToClassPath(new Path("stanford-corenlp-4.0.0-models.jar"));
        annotateTweets.addFileToClassPath(new Path("lib/ejml-simple-0.38.jar"));
        annotateTweets.addFileToClassPath(new Path("lib/ejml-core-0.38.jar"));
        annotateTweets.addFileToClassPath(new Path("lib/ejml-ddense-0.38.jar"));
        annotateTweets.setJarByClass(AnnotateTweetsMapper.class);
        annotateTweets.setJobName("Annotate Tweets");

        annotateTweets.setMapperClass(AnnotateTweetsMapper.class);
        annotateTweets.setNumReduceTasks(0);

        annotateTweets.setOutputKeyClass(CreatedAtWritable.class);
        annotateTweets.setOutputValueClass(AnnotatedTweetWritable.class);

        FileInputFormat.addInputPath(annotateTweets, new Path(INPUT_ANNOTATION_FILE_PATH));
        FileOutputFormat.setOutputPath(annotateTweets, new Path(OUTPUT_ANNOTATION_FILE_PATH));

        annotateTweets.setInputFormatClass(SequenceFileInputFormat.class);
        annotateTweets.setOutputFormatClass(SequenceFileOutputFormat.class);

        return annotateTweets.waitForCompletion(true);
        // TODO(veenaarv): Add reducer class to rank tweets per day and choose top N tweets

    }

    public static boolean runWriteTweetsToParquetFile() throws Exception {
        Job writeTweetsToParquetFile = Job.getInstance();
        writeTweetsToParquetFile.addFileToClassPath(new Path("parquet-hadoop-bundle-1.9.0.jar"));
        writeTweetsToParquetFile.addFileToClassPath(new Path("parquet-hadoop-1.9.0.jar"));
        writeTweetsToParquetFile.addFileToClassPath(new Path("parquet-format-2.3.1.jar"));
        writeTweetsToParquetFile.addFileToClassPath(new Path("parquet-avro-1.9.0.jar"));
        writeTweetsToParquetFile.addFileToClassPath(new Path("avro-1.8.0.jar"));
        writeTweetsToParquetFile.setJarByClass(AnnotatedTweetToParquetMapper.class);
        writeTweetsToParquetFile.setJobName("WriteToParquet");

        writeTweetsToParquetFile.setMapperClass(AnnotatedTweetToParquetMapper.class);
        writeTweetsToParquetFile.setNumReduceTasks(0);

        writeTweetsToParquetFile.setOutputKeyClass(Void.class);
        writeTweetsToParquetFile.setOutputValueClass(Group.class);

        FileInputFormat.addInputPath(writeTweetsToParquetFile, new Path(OUTPUT_ANNOTATION_FILE_PATH));
        FileOutputFormat.setOutputPath(writeTweetsToParquetFile, new Path(OUTPUT_PARQUET_FILE_PATH));

        writeTweetsToParquetFile.setInputFormatClass(SequenceFileInputFormat.class);
        writeTweetsToParquetFile.setOutputFormatClass(AvroParquetOutputFormat.class);
        Schema schema = new Schema.Parser().parse(new File("annotated_tweet_schema.jsonl"));
        AvroParquetOutputFormat.setSchema(writeTweetsToParquetFile, schema);

        return writeTweetsToParquetFile.waitForCompletion(true);
    }

}
