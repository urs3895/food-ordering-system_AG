package com.food.ordering.system.order.service.messaging.listener.kafka;

import com.food.ordering.system.kafka.consumer.KafkaConsumer;
import com.food.ordering.system.kafka.order.avro.model.PaymentResponseAvroModel;
import com.food.ordering.system.kafka.order.avro.model.PaymentStatus;
import com.food.ordering.system.order.service.domain.exception.OrderNotFoundException;
import com.food.ordering.system.order.service.domain.ports.input.message.listener.payment.PaymentResponseMessageListener;
import com.food.ordering.system.order.service.messaging.mapper.OrderMessagingDataMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class PaymentResponseKafkaListener implements KafkaConsumer<PaymentResponseAvroModel> {

    private final PaymentResponseMessageListener paymentResponseMessageListener;
    private final OrderMessagingDataMapper orderMessagingDataMapper;

    public PaymentResponseKafkaListener(PaymentResponseMessageListener paymentResponseMessageListener,
                                        OrderMessagingDataMapper orderMessagingDataMapper) {
        this.paymentResponseMessageListener = paymentResponseMessageListener;
        this.orderMessagingDataMapper = orderMessagingDataMapper;
    }

    @Override
    @KafkaListener(id = "${kafka-consumer-config.payment-consumer-group-id}", topics = "${order-service.payment-response-topic-name}")
    public void receive(List<ConsumerRecord<String, PaymentResponseAvroModel>> records) {
        log.info("{} payment responses received", records.size());

        records.forEach(record -> {
            PaymentResponseAvroModel paymentResponseAvroModel = record.value();
            String key = record.key();
            int partition = record.partition();
            long offset = record.offset();

            log.info("Processing record with key: {}, partition: {}, offset: {}", key, partition, offset);

            try {
                if (PaymentStatus.COMPLETED == paymentResponseAvroModel.getPaymentStatus()) {
                    log.info("Processing successful payment for order id: {}", paymentResponseAvroModel.getOrderId());
                    paymentResponseMessageListener.paymentCompleted(orderMessagingDataMapper
                            .paymentResponseAvroModelToPaymentResponse(paymentResponseAvroModel));
                } else if (PaymentStatus.CANCELLED == paymentResponseAvroModel.getPaymentStatus()
                        || PaymentStatus.FAILED == paymentResponseAvroModel.getPaymentStatus()) {
                    log.info("Processing unsuccessful payment for order id: {}", paymentResponseAvroModel.getOrderId());
                    paymentResponseMessageListener.paymentCancelled(orderMessagingDataMapper
                            .paymentResponseAvroModelToPaymentResponse(paymentResponseAvroModel));
                }
            } catch (OptimisticLockingFailureException e) {
                log.error("Caught optimistic locking exception for order id: {}", paymentResponseAvroModel.getOrderId());
            } catch (OrderNotFoundException e) {
                log.error("No order found for order id: {}", paymentResponseAvroModel.getOrderId());
            }
        });
    }
}
