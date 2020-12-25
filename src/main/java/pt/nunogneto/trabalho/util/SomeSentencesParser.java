package pt.nunogneto.trabalho.util;

import java.util.ArrayList;

public class SomeSentencesParser implements DataParser {

    private static final ArrayList<String> publisherSentences = new ArrayList<>();

    private static final ArrayList<String> tags = new ArrayList<>();

    static {
        publisherSentences.add(
                "8% of 25 is the same as 25% of 8 and one of them is much easier to do in your head.");

        publisherSentences.add(
                "It was a really good Monday for being a Saturday.");

        publisherSentences.add(
                "The sunblock was handed to the girl before practice, but the burned skin was proof she did not apply it.");

        publisherSentences.add(
                "The skeleton had skeletons of his own in the closet.");

        tags.add("trial");
        tags.add("license");
        tags.add("support");
        tags.add("bug");
    }

    @Override
    public ArrayList<String> readPublisherSentences() {
        return publisherSentences;
    }

    @Override
    public ArrayList<String> readPossibleTags() {
        return tags;
    }
}
