package org.hps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {
    private static final Logger log = LogManager.getLogger(Producer.class);


    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        Workload wrld = new Workload();
        KafkaProducerConfig config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        KafkaProducer<String,Customer> producer = new KafkaProducer<>(props);
        Random rnd = new Random();
        // over all the workload
        for (int i = 0; i < wrld.getDatax().size(); i++) {
            log.info("sending a batch of authorizations of size:{}",
                    Math.ceil(wrld.getDatay().get(i)));
            //   loop over each sample
            for (long j = 0; j < Math.ceil(wrld.getDatay().get(i)); j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));

                //log.info("sleeping for {} seconds ms", (long)(1000/(Math.ceil(wrld.getDatay().get(i)))));
              //  Thread.sleep((long)(1000/(Math.ceil(wrld.getDatay().get(i)))));
            }
            log.info("sent {} events Per Second ", Math.ceil(wrld.getDatay().get(i)));

            Thread.sleep(delay);
        }
    }
}
