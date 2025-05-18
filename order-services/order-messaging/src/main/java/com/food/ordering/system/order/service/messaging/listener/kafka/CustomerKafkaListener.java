package com.food.ordering.system.order.service.messaging.listener.kafka;

import com.food.ordering.system.kafka.consumer.KafkaConsumer;
import com.food.ordering.system.kafka.order.avro.model.CustomerAvroModel;
import com.food.ordering.system.order.service.domain.ports.input.message.listener.customer.CustomerMessageListener;
import com.food.ordering.system.order.service.messaging.mapper.OrderMessagingDataMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class CustomerKafkaListener implements KafkaConsumer<CustomerAvroModel> {

    private final CustomerMessageListener customerMessageListener;
    private final OrderMessagingDataMapper orderMessagingDataMapper;

    public CustomerKafkaListener(CustomerMessageListener customerMessageListener,
                                 OrderMessagingDataMapper orderMessagingDataMapper) {
        this.customerMessageListener = customerMessageListener;
        this.orderMessagingDataMapper = orderMessagingDataMapper;
    }


    @Override
    @KafkaListener(id = "${kafka-consumer-config.customer-group-id}", topics = "${order-service.customer-topic-name}")
    public void receive(List<ConsumerRecord<String, CustomerAvroModel>> records) {
        log.info("{} number of customer messages received", records.size());
        records.forEach(record -> {
            customerMessageListener.customerCreated(
                    orderMessagingDataMapper.customerAvroModeltoCustomerModel(record.value())
            );
        });
    }


}
