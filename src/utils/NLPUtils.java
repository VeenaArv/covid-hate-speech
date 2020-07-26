package utils;

import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;

import java.util.Properties;
import writable.AnnotatedTweetWritable;
import writable.TweetWritable;

public class NLPUtils {
    public static void getSentiment(AnnotatedTweetWritable annotatedTweet) {
        CoreDocument document = new CoreDocument(annotatedTweet.tweet.rawText);
        Properties props = new Properties();
        props.setProperty("annotators", "sentiment");
        ParserAnnotator parserAnnotator = new ParserAnnotator("", props);
        parserAnnotator.annotate(document.annotation());

        SentimentAnnotator sn = new SentimentAnnotator("", props);
        sn.annotate(document.annotation());

        Tree sentimentTree = document.annotation().get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        annotatedTweet.sentimentScore = RNNCoreAnnotations.getPredictedClass(sentimentTree);
        annotatedTweet.sentimentProbability = RNNCoreAnnotations.getPredictedClassProb(sentimentTree.label());

    }

    public static void main(String[] args) {
        String text = "Covid19 destroyed my life. The United States sucks.";
        AnnotatedTweetWritable tweet = new AnnotatedTweetWritable(
                new TweetWritable(0l, text, "","","","", false));
        getSentiment(tweet);
        System.out.println(tweet.sentimentScore);
        System.out.println(tweet.sentimentProbability);

    }
}


