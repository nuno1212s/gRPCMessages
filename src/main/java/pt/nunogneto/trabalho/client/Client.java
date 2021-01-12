package pt.nunogneto.trabalho.client;

import pt.nunogneto.trabalho.BrokerGrpc;
import pt.nunogneto.trabalho.util.DataParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class Client {

    protected static final String TARGET = "localhost:5051";

    protected static Random random = new Random();

    private String tag;

    public Client(DataParser parser) {
        final ArrayList<String> tags = parser.readPossibleTags();

        this.tag = tags.get(random.nextInt(tags.size()));
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Client(DataParser parser, String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }
}
