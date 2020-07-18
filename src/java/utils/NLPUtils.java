package java.utils;

import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;

import java.util.Properties;
import java.writable.AnnotatedTweetWritable;

public class NLPUtils {
    public static void getSentiment(String text, AnnotatedTweetWritable annotatedTweet) {
        CoreDocument document = new CoreDocument(text);
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
}


