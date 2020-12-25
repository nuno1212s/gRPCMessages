package pt.nunogneto.trabalho.client;

import pt.nunogneto.trabalho.util.DataParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class Client {

    protected static final String TARGET = "localhost:5051";

    protected static Random random = new Random();

    private static List<String> randomMessages;

    private final String tag;

    public Client(DataParser parser) {

        randomMessages = parser.readPublisherSentences();

        final ArrayList<String> tags = parser.readPossibleTags();

        this.tag = tags.get(random.nextInt(tags.size()));
    }

    public static List<String> getRandomMessages() {
        return randomMessages;
    }

    public String getNextMessage() {
        return getRandomMessages().get(random.nextInt(getRandomMessages().size()));
    }

    public String getTag() {
        return tag;
    }
}
