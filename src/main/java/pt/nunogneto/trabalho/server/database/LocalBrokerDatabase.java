package pt.nunogneto.trabalho.server.database;

import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.MessageToPublish;
import pt.nunogneto.trabalho.TagMessage;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class LocalBrokerDatabase implements BrokerDatabase {

    protected static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    protected static final Logger logger = Logger.getLogger(BrokerDatabase.class.getName());

    protected final Map<String, Collection<StreamObserver<MessageToPublish>>> publishers = new ConcurrentHashMap<>();

    protected final Map<String, Collection<StreamObserver<TagMessage>>> subscribers = new ConcurrentHashMap<>();

    public LocalBrokerDatabase() {
        executor.scheduleAtFixedRate(this::sendKeepAliveToSubscribers, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void sendKeepAliveToSubscribers() {
        subscribers.forEach((tag, tagSubs)  -> {

            final TagMessage keepAlive = TagMessage.newBuilder().setIsKeepAlive(true).build();

            final Iterator<StreamObserver<TagMessage>> iterator = tagSubs.iterator();

            while (iterator.hasNext()) {
                final StreamObserver<TagMessage> next = iterator.next();

                try {
                    next.onNext(keepAlive);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Client for tag {0} has disconnected.", tag);

                    iterator.remove();
                }
            }

        });
    }

    @Override
    public List<String> getTagList() {
        return new ArrayList<>(publishers.keySet());
    }

    @Override
    public Map<String, Collection<StreamObserver<MessageToPublish>>> getActivePublishers() {
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
    public void registerPublisher(String tag, StreamObserver<MessageToPublish> stream) {

        Collection<StreamObserver<MessageToPublish>> publishers = this.publishers.getOrDefault(tag, new LinkedList<>());

        publishers.add(stream);

        this.publishers.put(tag, publishers);

    }

    @Override
    public void removePublisher(StreamObserver<MessageToPublish> publisher) {
        for (Map.Entry<String, Collection<StreamObserver<MessageToPublish>>> tag
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

        final Iterator<StreamObserver<TagMessage>> iterator = streamObservers.iterator();

        while (iterator.hasNext()) {

            final StreamObserver<TagMessage> stream = iterator.next();

            try {
                stream.onNext(build);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Client for tag {0} has disconnected", toPublish.getTag());

                iterator.remove();
            }
        }
    }

    @Override
    public void shutdown() {
        publishers.forEach((tag, publisherStreams) -> {

            logger.log(Level.WARNING, "Disconnecting publishers for tag {0}", tag);

            try {
                for (StreamObserver<MessageToPublish> publisherStream : publisherStreams) {
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
    }
}
