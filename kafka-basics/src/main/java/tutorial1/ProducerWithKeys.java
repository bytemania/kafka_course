package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

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
                String topic = "first_topic";
                String value = "hello world " + i;
                String key = "id_" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("Received new metadata. \nTopic:{} \n Partition:{} Offeset:{} Timestamp:{}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }).get();
            }
            producer.flush();
        }
    }
}
