package org.adex.wikimediamsproducer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class WikimediaMsProducerApplication implements CommandLineRunner {

    @Autowired
    private WikimediaProducer<String> wikimediaProducerImpl;

    public static void main(String[] args) {
        SpringApplication.run(WikimediaMsProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        wikimediaProducerImpl.sendMessage();
    }
}

@Configuration
@Getter
class Conf {

    @Value("${wikimedia.ms.topic.name}")
    private String wikimediaTopicName;

    @Value("${wikimedia.ms.url}")
    private String wikimediaUrl;

    @Bean
    public NewTopic wikimediaTopic() {
        return TopicBuilder
                .name(wikimediaTopicName)
                .build();
    }

}

interface WikimediaProducer<T> {

    void sendMessage() throws InterruptedException;

}

@Service
@Slf4j
@Setter
class WikimediaProducerImpl implements WikimediaProducer<String> {


    private final KafkaTemplate<String, String> kafkaTemplate;

    private Conf conf;

    public WikimediaProducerImpl(KafkaTemplate<String, String> kafkaTemplate, Conf conf) {
        this.kafkaTemplate = kafkaTemplate;
        this.conf = conf;
    }

    @Override
    public void sendMessage() throws InterruptedException {
        EventHandler eventHandler = new WikimediaEventHandlerImpl(kafkaTemplate, conf.getWikimediaTopicName());
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(eventHandler, URI.create(conf.getWikimediaUrl()));
        EventSource eventSource = eventSourceBuilder.build();
        eventSource.start();
        TimeUnit.SECONDS.sleep(20);
    }
}

@Slf4j
@AllArgsConstructor
class WikimediaEventHandlerImpl implements EventHandler {

    private KafkaTemplate<String, String> kafkaTemplate;

    private String topic;

    @Override
    public void onOpen() throws Exception {
        log.info("==> on open");
    }

    @Override
    public void onClosed() throws Exception {
        log.info("==> on closed");
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(String.format("==> Message sent %s : ", messageEvent.getData()));
        this.kafkaTemplate.send(topic, messageEvent.getData());
    }

    @Override
    public void onComment(String s) throws Exception {
        log.info("==> on comment");
    }

    @Override
    public void onError(Throwable throwable) {
        log.info("==> on error");
    }
}