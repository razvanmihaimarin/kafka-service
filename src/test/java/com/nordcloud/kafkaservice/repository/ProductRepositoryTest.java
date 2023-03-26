package com.nordcloud.kafkaservice.repository;


import com.nordcloud.kafkaservice.model.entity.Product;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


@Testcontainers
@ExtendWith(SpringExtension.class)
@DataR2dbcTest
@ActiveProfiles("test")
public class ProductRepositoryTest {

    public static final String TEST_PRODUCT = "Test Product";
    public static final String TEST_DESCRIPTION = "Test Description";
    public static final double TEST_PRICE = 10D;
    public static final String UUID = "1-2-3-4";

    @Autowired
    private ProductRepository repository;

    @Test
    void givenProduct_whenSaveAndFind_thenReturnSavedProduct(){
        // given
        Product inserted = repository.save(buildProduct()).block();

        // when
        Mono<Product> ProductMono = repository.findById(inserted.getId());

        // then
        StepVerifier
                .create(ProductMono)
                .assertNext(product -> {
                    Assertions.assertNotNull(product.getId());
                    Assertions.assertEquals(TEST_PRODUCT, product.getName());
                    Assertions.assertEquals(TEST_DESCRIPTION, product.getDescription());
                    Assertions.assertEquals(Double.valueOf(TEST_PRICE), product.getPrice());
                })
                .expectComplete()
                .verify();
    }

    private Product buildProduct(){
        Product product = new Product();
        product.setUuid(UUID);
        product.setName(TEST_PRODUCT);
        product.setDescription(TEST_DESCRIPTION);
        product.setPrice(TEST_PRICE);

        return product;
    }
}
