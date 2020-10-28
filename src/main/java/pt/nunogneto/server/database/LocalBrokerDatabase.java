package pt.nunogneto.server.database;

import io.grpc.stub.StreamObserver;
import pt.nunogneto.MessageToPublish;
import pt.nunogneto.TagMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class LocalBrokerDatabase implements BrokerDatabase {

    protected final Map<String, Collection<StreamObserver<MessageToPublish>>> publishers = new ConcurrentHashMap<>();

    protected final Map<String, Collection<StreamObserver<TagMessage>>> subscribers = new ConcurrentHashMap<>();

    @Override
    public List<String> getTagList() {
        return new ArrayList<>(publishers.keySet());
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

        streamObservers.forEach(stream -> stream.onNext(build));
    }

}
