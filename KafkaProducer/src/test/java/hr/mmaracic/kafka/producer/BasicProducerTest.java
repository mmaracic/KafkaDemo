/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.mmaracic.kafka.producer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 *
 * @author mmaracic
 */
@SpringBootTest
public class BasicProducerTest {
    
    @Autowired
    private ApplicationContext applicationContext;
    
    @Test
    public void basicContextTest() {
        Assertions.assertNotNull(applicationContext);
    }
}
