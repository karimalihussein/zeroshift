package io.zeroshift.shipping;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.ShippingCommand.ScheduleShipment;
import io.zeroshift.contracts.ShippingEvent.*;
import io.zeroshift.contracts.Topics;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.testing.CommerceStack;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ShippingServiceIT {
  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    configure(registry, "shipping");
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired Faults faults;

  @BeforeEach
  void connector() { // after the service's migrations created the publication and slot
    CommerceStack.registerOutboxConnector("shipping");
  }

  @Test
  void schedulesOnceAndAnswersARepeatWithTheSameTrackingNumber() {
    var order = UUID.randomUUID();
    send(kafka, new ScheduleShipment(order, "c-1"));
    send(kafka, new ScheduleShipment(order, "c-1"));

    var replies = replies(Topics.SHIPPING_EVENTS, order, 2);
    var first = (ShipmentScheduled) replies.get(0).payload();
    assertThat(((ShipmentScheduled) replies.get(1).payload()).trackingNumber())
        .isEqualTo(first.trackingNumber());
  }

  @Test
  void anArmedFaultFailsTheShipment() {
    faults.arm(ShippingHandler.FAIL_FAULT, "fail", 1);
    var order = UUID.randomUUID();
    send(kafka, new ScheduleShipment(order, "c-2"));

    assertThat(replies(Topics.SHIPPING_EVENTS, order, 1).getFirst().payload())
        .isInstanceOf(ShipmentFailed.class);
  }
}
