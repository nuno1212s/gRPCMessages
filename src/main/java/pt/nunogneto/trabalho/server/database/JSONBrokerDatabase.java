package pt.nunogneto.trabalho.server.database;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.MessageToPublish;
import pt.nunogneto.trabalho.TagMessage;
import pt.nunogneto.trabalho.util.DataParser;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class JSONBrokerDatabase extends LocalBrokerDatabase {

    private static final String STORAGE_NAME = "storage.json";

    private static final String ID = "ID", DATE = "D", PUBLISH_TAG = "T", MESSAGE = "M";

    private static final Executor executor = Executors.newSingleThreadExecutor();

    private final File file;

    private final long expirationTime;

    private final ReentrantLock messagesLock = new ReentrantLock();

    private SortedSet<PublishedMessage> publishedMessages = new TreeSet<>();

    public JSONBrokerDatabase(long expirationTime, DataParser parser) {
        super(parser);
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

        try {
            messagesLock.lock();

            while (reader.hasNext()) {
                reader.beginObject();

                this.publishedMessages.add(readMessage(reader));

                reader.endObject();
            }

        } finally {
            messagesLock.unlock();
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

        List<TagMessage> toSendToUser = new LinkedList<>();

        try {
            messagesLock.lock();

            Iterator<PublishedMessage> iterator = publishedMessages.iterator();

            while (iterator.hasNext()) {

                PublishedMessage next = iterator.next();

                if (next.hasExpired(this.expirationTime)) {
                    //Since this is a sorted set all the messages from here on out won't be delivered, as they've expired
                    publishedMessages = publishedMessages.headSet(next);

                    asyncSave();
                    break;
                }

                if (!next.getTag().equalsIgnoreCase(tag)) {
                    continue;
                }

                toSendToUser.add(TagMessage.newBuilder().setMessage(next.getMessage())
                        .setDate(next.getPublishedDate())
                        .setOriginatingTag(next.getTag())
                        .setTagID(next.getMessageID())
                        .setIsKeepAlive(false).build());
            }
        } finally {
            messagesLock.unlock();
        }

        //The messages are ordered newest first, but we want the user to receive the oldest messages first
        Collections.reverse(toSendToUser);

        publishMessageListToClient(stream, toSendToUser, tag).whenComplete((result, excep) -> {

            if (!result) {
                removeSubscriber(tag, stream);
            }

        });
    }

    @Override
    public void publishMessage(MessageToPublish toPublish) {
        super.publishMessage(toPublish);

        PublishedMessage publishedMessage = new PublishedMessage(toPublish.getDate(), toPublish.getId(),
                toPublish.getTag(), toPublish.getMessage());

        try {
            messagesLock.lock();

            publishedMessages.add(publishedMessage);

        } finally {
            messagesLock.unlock();
        }

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

            try {
                messagesLock.lock();

                for (PublishedMessage publishedMessage : publishedMessages) {
                    publishedMessage.writeTo(writer);
                }

            } finally {
                messagesLock.unlock();
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

        public long getMessageID() {
            return messageID;
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
            return -Long.compare(publishedDate, publishedMessage.getPublishedDate());
        }
    }
}
