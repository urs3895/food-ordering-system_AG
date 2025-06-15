package com.food.ordering.system.order.service.domain.dto.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

@Getter
@Builder
@AllArgsConstructor
public class CustomerModel {
    private UUID id;
    private String username;
    private String firstName;
    private String lastName;
}
