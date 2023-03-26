package com.nordcloud.kafkaservice.mapper;

import com.nordcloud.kafkaservice.model.dto.ProductDto;
import com.nordcloud.kafkaservice.model.entity.Product;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    Product toProduct(ProductDto productDto);
}
