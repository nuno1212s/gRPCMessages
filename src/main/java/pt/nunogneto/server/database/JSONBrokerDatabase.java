package pt.nunogneto.server.database;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.grpc.stub.StreamObserver;
import org.omg.IOP.TAG_ALTERNATE_IIOP_ADDRESS;
import pt.nunogneto.MessageToPublish;
import pt.nunogneto.TagMessage;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class JSONBrokerDatabase extends LocalBrokerDatabase {

    private static final String STORAGE_NAME = "storage.json";

    private static final String ID = "ID", DATE = "D", PUBLISH_TAG = "T", MESSAGE = "M";

    private static final Executor executor = Executors.newSingleThreadExecutor();

    private final File file;

    private SortedSet<PublishedMessage> publishedMessages = new TreeSet<>();

    private final long expirationTime;

    public JSONBrokerDatabase(long expirationTime) {

        this.expirationTime = expirationTime;

        this.file = new File(STORAGE_NAME);

        if (file.exists()) {

            try {
                JsonReader reader = new JsonReader(new FileReader(file));

                readFile(reader);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void readFile(JsonReader reader) throws IOException {

        reader.beginArray();

        while (reader.hasNext()) {
            reader.beginObject();

            this.publishedMessages.add(readMessage(reader));

            reader.endObject();
        }

        reader.endArray();
    }

    private PublishedMessage readMessage(JsonReader reader) throws IOException {

        long publishedDate = 0, id = 0;

        String tag = null, message = null;

        while (reader.hasNext()) {

            switch (reader.nextName()) {
                case DATE:
                    publishedDate = reader.nextLong();
                    break;
                case ID:
                    id = reader.nextLong();
                    break;
                case PUBLISH_TAG:
                    tag = reader.nextString();
                    break;
                case MESSAGE:
                    message = reader.nextString();
                    break;
                default:
                    reader.skipValue();
                    break;
            }

        }

        return new PublishedMessage(publishedDate, id, tag, message);
    }

    @Override
    public void registerSubscriber(String tag, StreamObserver<TagMessage> stream) {
        super.registerSubscriber(tag, stream);

        Iterator<PublishedMessage> iterator = publishedMessages.iterator();

        while (iterator.hasNext()) {

            PublishedMessage next = iterator.next();

            if (next.hasExpired(this.expirationTime)) {
                //Since this is a sorted set all these message won't be delivered
                publishedMessages = publishedMessages.headSet(next);

                asyncSave();
                break;
            }

            if (!next.getTag().equalsIgnoreCase(tag)) {
                continue;
            }

            stream.onNext(TagMessage.newBuilder().setMessage(next.getMessage()).build());
        }
    }

    @Override
    public void publishMessage(MessageToPublish toPublish) {
        super.publishMessage(toPublish);

        PublishedMessage publishedMessage = new PublishedMessage(toPublish.getDate(), toPublish.getId(),
                toPublish.getTag(), toPublish.getMessage());

        publishedMessages.add(publishedMessage);

        asyncSave();
    }

    private void asyncSave() {
        executor.execute(this::save);
    }

    private void save() {

        try {

            if (!file.exists())
                file.createNewFile();

            JsonWriter writer = new JsonWriter(new FileWriter(file));

            writer.beginArray();

            for (PublishedMessage publishedMessage : publishedMessages) {
                publishedMessage.writeTo(writer);
            }

            writer.endArray();

            writer.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class PublishedMessage implements Comparable<PublishedMessage> {

        private final long publishedDate;

        private final long messageID;

        private final String tag, message;

        public PublishedMessage(long publishedDate, long messageID, String tag, String message) {
            this.publishedDate = publishedDate;
            this.messageID = messageID;
            this.tag = tag;
            this.message = message;
        }

        public long getPublishedDate() {
            return publishedDate;
        }

        public String getTag() {
            return tag;
        }

        public String getMessage() {
            return message;
        }

        protected boolean hasExpired(long expirationTime) {
            return publishedDate + expirationTime < System.currentTimeMillis();
        }

        protected void writeTo(JsonWriter writer) throws IOException {

            writer.beginObject();

            writer.name(DATE);

            writer.value(publishedDate);

            writer.name(PUBLISH_TAG);

            writer.value(tag);

            writer.name(MESSAGE);

            writer.value(message);

            writer.name(ID);

            writer.value(messageID);

            writer.endObject();

        }

        @Override
        public int compareTo(PublishedMessage publishedMessage) {
            return Long.compare(publishedDate, publishedMessage.getPublishedDate());
        }
    }
}
