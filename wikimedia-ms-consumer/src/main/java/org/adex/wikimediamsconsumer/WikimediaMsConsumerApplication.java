package org.adex.wikimediamsconsumer;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import javax.persistence.*;

@SpringBootApplication
public class WikimediaMsConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WikimediaMsConsumerApplication.class, args);
    }

}

@Entity
@Getter
@Setter
class Wikimedia {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Lob
    private String changes;

}

@Repository
interface WikimediaDao extends CrudRepository<Wikimedia, Long> {

}

interface WikimediaConsumer<T> {

    void consume(String eventMessage);

}

@Service
@Slf4j
class WikimediaConsumerImpl implements WikimediaConsumer<String> {

    @Autowired
    private WikimediaDao wikimediaDao;

    @Override
    @KafkaListener(
            topics = "${wikimedia.ms.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(String eventMessage) {
        log.info("==> message received %s : " + eventMessage);

        Wikimedia wikimedia = new Wikimedia();
        wikimedia.setChanges(eventMessage);

        this.wikimediaDao.save(wikimedia);
    }
}