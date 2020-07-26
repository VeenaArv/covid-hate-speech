import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import writable.CreatedAtWritable;
import writable.TweetWritable;

public class PipelineMain {
    static final String INPUT_TWEETS_FILE_PATH = "covid/data/coronavirus-through-09-June-2020-00.jsonl";
    static final String OUTPUT_TWEETS_FILE_PATH = "covid/out/";
    static final String INPUT_ANNOTATION_FILE_PATH = OUTPUT_TWEETS_FILE_PATH;
    static final String OUTPUT_ANNOTATION_FILE_PATH = "covid/annotation/out";


    public static void main(String[] args) throws Exception {
        System.exit(runExtractTweets() ? 0 : 1);

    }

    public static boolean runExtractTweets() throws Exception {
        Job extractTweets = Job.getInstance();
        extractTweets.setJarByClass(JsonToTweetMapper.class);
        extractTweets.setJobName("Json to Tweet Mapper");
        FileInputFormat.addInputPath(extractTweets, new Path(INPUT_TWEETS_FILE_PATH));
        FileOutputFormat.setOutputPath(extractTweets, new Path(OUTPUT_TWEETS_FILE_PATH));

        extractTweets.setMapperClass(JsonToTweetMapper.class);
        extractTweets.setMapOutputKeyClass(TweetWritable.class);
        extractTweets.setMapOutputValueClass(TweetWritable.class);
        extractTweets.setReducerClass(RemoveDuplicatesReducer.class);
        extractTweets.setOutputKeyClass(CreatedAtWritable.class);
        extractTweets.setOutputValueClass(TweetWritable.class);
        return extractTweets.waitForCompletion(true);
    }

    public static void runAnnotateTweets() throws IOException {
        Job annotateTweets = Job.getInstance();
        annotateTweets.setJarByClass(AnnotateTweetsMapper.class);
        annotateTweets.setJobName("Annotate Tweets");
        FileInputFormat.addInputPath(annotateTweets, new Path((INPUT_ANNOTATION_FILE_PATH)));
        FileOutputFormat.setOutputPath(annotateTweets, new Path(OUTPUT_ANNOTATION_FILE_PATH));
        annotateTweets.setMapOutputKeyClass(CreatedAtWritable.class);
        annotateTweets.setMapOutputValueClass(TweetWritable.class);
        // TODO(veenaarv): Add reducer class to rank tweets per day and choose top N tweets

    }

}
