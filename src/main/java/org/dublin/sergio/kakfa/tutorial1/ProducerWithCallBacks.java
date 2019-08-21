package org.dublin.sergio.kakfa.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBacks {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallBacks.class);

        String bootstrapserver = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 10; i++){
                //Send data
                ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("Received new metadata. \nTopic:{} \n Partition:{} Offeset:{} Timestamp:{}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                });
            }

            producer.flush();
        }

    }

}
