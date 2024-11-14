package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private String topic;
    private KafkaTemplate<String, Transaction> kafkaTemplate;


    // Created a no Args constructor as well as made the fields non-final: Solved the following error:
    // org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'kafkaProducer' defined in file
    // [A:\Project Saves\forage-midas\target\test-classes\com\jpmc\midascore\KafkaProducer.class]: Unexpected exception during bean creation
    // Interestingly. I've tried using the annotation, @Autowired, to solve this but to no avail.
    // The issue I'm having is that Spring is SEARCHING for a no-args constructor, to which this class did not have one. Thus, creating the
    // BeanCreatingException error message I was facing. Hoping this was part of the test! Very interested to learn more

    public KafkaProducer() {

    }

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }


    public void send(String transactionLine) {
        String[] transactionData = transactionLine.split(", ");
        kafkaTemplate.send(topic, new Transaction(Long.parseLong(transactionData[0]), Long.parseLong(transactionData[1]), Float.parseFloat(transactionData[2])));
    }
}