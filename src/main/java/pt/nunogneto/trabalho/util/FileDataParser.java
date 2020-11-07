package pt.nunogneto.trabalho.util;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

public class FileDataParser implements DataParser {

    private static final String PUBLISHER_FILE = "publishersentences.txt",
            RANDOM_TAGS = "randomtags.txt";

    private InputStream readResource(String fileName) {
        return FileDataParser.class.getResourceAsStream(fileName);
    }

    private BufferedReader openReader(InputStream streamResource) {
        return new BufferedReader(new InputStreamReader(streamResource));
    }

    private ArrayList<String> readFrom(String file) {
        LinkedList<String> possibleTags = new LinkedList<>();

        try (InputStream inputStream = readResource(file);
             BufferedReader bufferedReader = openReader(inputStream)) {

            String currentLine;

            while ((currentLine = bufferedReader.readLine()) != null) {
                possibleTags.add(currentLine);
            }

            return new ArrayList<>(possibleTags);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public ArrayList<String> readPossibleTags() {
        return readFrom(RANDOM_TAGS);
    }

    @Override
    public ArrayList<String> readPublisherSentences() {
        return readFrom(PUBLISHER_FILE);
    }
}
