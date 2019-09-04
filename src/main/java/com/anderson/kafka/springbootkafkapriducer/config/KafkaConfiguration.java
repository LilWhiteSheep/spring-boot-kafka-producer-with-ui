package com.anderson.kafka.springbootkafkapriducer.config;

import com.anderson.kafka.springbootkafkapriducer.model.User;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Bean
    public ProducerFactory producerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "140.118.109.106:9092");
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //TestFileTransfer
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
       //TestFileTransferEnd

        //JsonVer
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        

        return new DefaultKafkaProducerFactory(config);
    }
    @Bean
    public KafkaTemplate<Integer, byte[]> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    //Json
//    public KafkaTemplate<String, User> kafkaTemplate() {
//        return new KafkaTemplate<>(producerFactory());
//    }
    //String
//    public KafkaTemplate<String, String> kafkaTemplate() {
//        return new KafkaTemplate<>(producerFactory());
//    }
}
