package pt.nunogneto.trabalho.server.database;

import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.KeepAlive;
import pt.nunogneto.trabalho.MessageToPublish;
import pt.nunogneto.trabalho.TagMessage;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LocalBrokerDatabase implements BrokerDatabase {

    private static final List<String> defaultTags = Arrays.asList("trial", "license", "support", "bug");

    private static final int DEFAULT_EXECUTORS = 5;

    protected static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    protected static final ExecutorService executors = Executors.newFixedThreadPool(DEFAULT_EXECUTORS);

    protected static final Logger logger = Logger.getLogger(BrokerDatabase.class.getName());

    protected final Map<String, Collection<StreamObserver<KeepAlive>>> publishers = new ConcurrentHashMap<>();

    protected final Map<String, Collection<StreamObserver<TagMessage>>> subscribers = new ConcurrentHashMap<>();

    public LocalBrokerDatabase() {
        //Create a thread that sends keep alives for every client every 1 second. This is to check whether the client
        //Has disconnected from the server, without blocking the main thread that listens to messages

        for (String defaultTag : defaultTags) {
            publishers.put(defaultTag, new LinkedList<>());
            subscribers.put(defaultTag, new LinkedList<>());
        }

        //Start the executors
        executor.scheduleAtFixedRate(this::sendKeepAliveToSubscribers, (long) (Math.random() * 1000L), 1000, TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(this::sendKeepAliveToPublishers, (long) (Math.random() * 1000L), 1000, TimeUnit.MILLISECONDS);
    }

    private void sendKeepAliveToSubscribers() {
        subscribers.forEach((tag, tagSubs) -> {

            final TagMessage keepAlive = TagMessage.newBuilder().setIsKeepAlive(true).build();

            publishAndHandleDisconnectedClients(tagSubs, keepAlive, tag);
        });
    }

    private void sendKeepAliveToPublishers() {
        publishers.forEach((tag, publishersForTag) -> {

            final KeepAlive keepAlive = KeepAlive.newBuilder().build();

            publishAndHandleDisconnectedClients(publishersForTag, keepAlive, tag);
        });
    }

    @Override
    public List<String> getTagList() {
        return new ArrayList<>(publishers.keySet());
    }

    @Override
    public Map<String, Collection<StreamObserver<KeepAlive>>> getActivePublishers() {
        return ImmutableMap.copyOf(this.publishers);
    }

    @Override
    public Map<String, Collection<StreamObserver<TagMessage>>> getActiveSubscribers() {
        return ImmutableMap.copyOf(this.subscribers);
    }

    @Override
    public void registerSubscriber(String tag, StreamObserver<TagMessage> stream) {

        Collection<StreamObserver<TagMessage>> orDefault = subscribers.getOrDefault(tag, new LinkedList<>());

        orDefault.add(stream);

        subscribers.put(tag, orDefault);
    }

    @Override
    public void registerPublisher(String tag) {
        this.publishers.put(tag, new LinkedList<>());
    }

    @Override
    public void registerPublisher(String tag, StreamObserver<KeepAlive> stream) {

        Collection<StreamObserver<KeepAlive>> publishers = this.publishers.getOrDefault(tag, new LinkedList<>());

        publishers.add(stream);

        this.publishers.put(tag, publishers);

    }

    @Override
    public void removePublisher(StreamObserver<KeepAlive> publisher) {
        for (Map.Entry<String, Collection<StreamObserver<KeepAlive>>> tag
                : publishers.entrySet()) {

            if (tag.getValue().remove(publisher)) {
                if (tag.getValue().isEmpty()) {
                    publishers.remove(tag.getKey());
                }

                break;
            }
        }
    }

    @Override
    public void publishMessage(MessageToPublish toPublish) {
        Collection<StreamObserver<TagMessage>> streamObservers = subscribers.get(toPublish.getTag());

        if (streamObservers == null) return;

        TagMessage build = TagMessage.newBuilder().setTagID(toPublish.getId())
                .setDate(toPublish.getDate())
                .setOriginatingTag(toPublish.getTag())
                .setMessage(toPublish.getMessage()).build();

        publishAndHandleDisconnectedClients(streamObservers, build, toPublish.getTag());
    }

    private <T> void publishAndHandleDisconnectedClients(
            Collection<StreamObserver<T>> streamObservers,
            T toPublish, String tag) {

        final CompletableFuture<AbstractMap.SimpleEntry<StreamObserver<T>, Boolean>>[] resultsForClient
                = publishMessageInList(streamObservers, toPublish, tag);

        CompletableFuture.allOf(resultsForClient).whenComplete((void_, exception) -> {
            if (exception != null) {
                exception.printStackTrace(System.out);

                return;
            }

            for (CompletableFuture<AbstractMap.SimpleEntry<StreamObserver<T>, Boolean>> futureResult : resultsForClient) {

                final AbstractMap.SimpleEntry<StreamObserver<T>, Boolean> result = futureResult.join();

                if (!result.getValue()) {
                    //Remove the disconnected clients from the client list
                    streamObservers.remove(result.getKey());
                }

            }
        });
    }

    /**
     * Returns the pair of the stream and the result of publishing to it wrapped in a completable future
     *
     * @param streamObservers
     * @param toPublish
     * @param tag
     * @param <T>
     * @return
     */
    private <T> CompletableFuture<AbstractMap.SimpleEntry<StreamObserver<T>, Boolean>>[] publishMessageInList(
            Collection<StreamObserver<T>> streamObservers,
            T toPublish, String tag) {

        CompletableFuture<AbstractMap.SimpleEntry<StreamObserver<T>, Boolean>>[] futures = new CompletableFuture[streamObservers.size()];

        final Iterator<StreamObserver<T>> iterator = streamObservers.iterator();

        int i = 0;
        while (iterator.hasNext()) {
            final StreamObserver<T> next = iterator.next();

            CompletableFuture<AbstractMap.SimpleEntry<StreamObserver<T>, Boolean>> publishTask =
                    CompletableFuture.supplyAsync(() ->
                            new AbstractMap.SimpleEntry<>(next, sendMessageToClient(next, toPublish, tag)),
                            executors);

            futures[i] = publishTask;
        }

        return futures;
    }

    /**
     * Attempt to send a message to a client.
     *
     * Returns whether the message was sent successfully or not
     *
     * @param stream The client to send to
     * @param toPublish The message to send
     * @param tag The tag of the message
     * @return
     */
    private <T> boolean sendMessageToClient(StreamObserver<T> stream, T toPublish, String tag) {

        try {
            stream.onNext(toPublish);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Client for tag {0} has disconnected", tag);

            return false;
        }

        return true;
    }

    @Override
    public void shutdown() {

        //Block the main thread to notify all clients correctly and only allow the server to shutdown
        publishers.forEach((tag, publisherStreams) -> {

            logger.log(Level.WARNING, "Disconnecting publishers for tag {0}", tag);

            try {
                for (StreamObserver<KeepAlive> publisherStream : publisherStreams) {
                    publisherStream.onCompleted();
                }
            } catch (Exception ignored) {

            }

        });

        subscribers.forEach((tag, subscriberStreams) -> {

            try {
                logger.log(Level.WARNING, "Disconnecting subscribers for tag {0}", tag);

                for (StreamObserver<TagMessage> subscriberStream : subscriberStreams) {
                    subscriberStream.onCompleted();
                }
            } catch (Exception ignored) {

            }
        });

        executors.shutdown();
        executor.shutdown();
    }
}
