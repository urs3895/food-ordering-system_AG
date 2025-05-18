package com.food.ordering.system.restaurant.service.messaging.listener.kafka;

import com.food.ordering.system.kafka.consumer.KafkaConsumer;
import com.food.ordering.system.kafka.order.avro.model.RestaurantApprovalRequestAvroModel;
import com.food.ordering.system.restaurant.service.domain.ports.input.message.listener.RestaurantApprovalRequestMessageListener;
import com.food.ordering.system.restaurant.service.messaging.mapper.RestaurantMessagingDataMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class RestaurantApprovalRequestKafkaListener implements KafkaConsumer<RestaurantApprovalRequestAvroModel> {

    private final RestaurantApprovalRequestMessageListener restaurantApprovalRequestMessageListener;
    private final RestaurantMessagingDataMapper restaurantMessagingDataMapper;

    public RestaurantApprovalRequestKafkaListener(RestaurantApprovalRequestMessageListener
                                                          restaurantApprovalRequestMessageListener,
                                                  RestaurantMessagingDataMapper
                                                          restaurantMessagingDataMapper) {
        this.restaurantApprovalRequestMessageListener = restaurantApprovalRequestMessageListener;
        this.restaurantMessagingDataMapper = restaurantMessagingDataMapper;
    }

    @Override
    @KafkaListener(id = "${kafka-consumer-config.restaurant-approval-consumer-group-id}",
            topics = "${restaurant-service.restaurant-approval-request-topic-name}")
    public void receive(List<ConsumerRecord<String, RestaurantApprovalRequestAvroModel>> records) {
        log.info("{} number of order approval requests received, sending for restaurant approval", records.size());

        records.forEach(record -> {
            RestaurantApprovalRequestAvroModel restaurantApprovalRequestAvroModel = record.value();
            String key = record.key();
            int partition = record.partition();
            long offset = record.offset();

            log.info("Processing order approval for order id: {} | key: {} | partition: {} | offset: {}",
                    restaurantApprovalRequestAvroModel.getOrderId(), key, partition, offset);

            restaurantApprovalRequestMessageListener.approveOrder(
                    restaurantMessagingDataMapper.restaurantApprovalRequestAvroModelToRestaurantApproval(
                            restaurantApprovalRequestAvroModel
                    )
            );
        });
    }

}
