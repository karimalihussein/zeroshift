package io.zeroshift.eventlab;

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class EventTapTest {
  @Test
  void decodesTextAndSpringsBinaryNumberHeaders() {
    assertThat(EventTap.header("OrderPlaced".getBytes(StandardCharsets.UTF_8)))
        .isEqualTo("OrderPlaced");
    assertThat(EventTap.header(ByteBuffer.allocate(4).putInt(5).array())).isEqualTo("5");
    assertThat(EventTap.header(ByteBuffer.allocate(8).putLong(1234567L).array()))
        .isEqualTo("1234567");
    assertThat(EventTap.header(new byte[] {0, 1, 2})).isEqualTo("0x000102");
    assertThat(EventTap.header(null)).isNull();
  }

  @Test
  void neverStoresNul() {
    assertThat(EventTap.clean("a\u0000b")).doesNotContain("\u0000").startsWith("a").endsWith("b");
  }
}
