package xiaoyf.demo.schemaregistry.producer.basic;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import xiaoyf.demo.schemaregistry.model.Logika;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Properties;

import static xiaoyf.demo.schemaregistry.helper.ProducerHelper.defaultProperties;

/*
 Purpose: to test LogicalType console consumer
 */
public class LogicalTypeProducer {

    public static void main(String[] args) throws Exception {
        Properties props = defaultProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        KafkaProducer<Logika, Logika> producer = new KafkaProducer<>(props);

        Logika logika = Logika.newBuilder()
                .setMyDecimal(new BigDecimal("2.38"))
                .setMyInstant(Instant.ofEpochMilli(200))
                .setMySet("A,B")
                .build();

        ProducerRecord<Logika, Logika> record = new ProducerRecord<>("logika", logika, logika);
        producer.send(record).get();
        producer.flush();
        producer.close();
    }
}
