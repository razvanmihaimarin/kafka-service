package com.nordcloud.kafkaservice.model.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    @Id
    private Integer id;
    @Column("uuid")
    private String uuid;
    @Column("p_name")
    private String name;
    private String description;
    private Double price;
}