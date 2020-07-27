package us.utils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import us.writable.AnnotatedTweetWritable;
import us.writable.TweetWritable;


import java.util.Properties;
public class NLPUtils {
    public static void populateSentiment(AnnotatedTweetWritable annotatedTweet) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        // create an empty Annotation just with the given text
        Annotation annotation = new Annotation(annotatedTweet.tweet.rawText);

        // run all Annotators on this text
        pipeline.annotate(annotation);

        double sentimentScoreSum = 0, sentimentProbSum = 0, numSentences = 0;
        int minSentimentScore = 4;
        double minSentimentProb = 1;
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree sentimentTree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(sentimentTree);
            double sentimentProb  = RNNCoreAnnotations.getPredictedClassProb(sentimentTree.label());
            // average strategy.
            sentimentScoreSum += sentiment;
            sentimentProbSum += sentimentProb;
            numSentences++;
            // min strategy.
            minSentimentScore = Integer.min(minSentimentScore, sentiment);
            minSentimentProb = Double.min(minSentimentProb, sentimentProb);
        }
        annotatedTweet.avgSentimentScore = (int) Math.round(sentimentScoreSum /numSentences);
        annotatedTweet.avgSentimentProbability = sentimentProbSum / numSentences;
        annotatedTweet.minSentimentScore = minSentimentScore;
        annotatedTweet.minSentimentProbability = minSentimentProb;
    }

    public static void main(String[] args) {

        String text = "Covid19 destroyed my life. The United States sucks.";
        AnnotatedTweetWritable tweet = new AnnotatedTweetWritable(
                new TweetWritable(0l, text, "","","","", false));
        populateSentiment(tweet);
        System.out.println(tweet.avgSentimentScore);
        System.out.println(tweet.avgSentimentProbability);

    }
}


