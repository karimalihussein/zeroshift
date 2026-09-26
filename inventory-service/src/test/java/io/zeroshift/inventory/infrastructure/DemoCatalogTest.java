package io.zeroshift.inventory.infrastructure;

import static org.assertj.core.api.Assertions.*;

import java.util.Random;
import org.junit.jupiter.api.Test;

class DemoCatalogTest {
  @Test
  void everyProductFitsTheSchema() {
    var products = DemoCatalog.generate(new Random());

    assertThat(products).extracting(DemoCatalog.DemoProduct::sku).doesNotHaveDuplicates();
    assertThat(products)
        .allSatisfy(
            p -> {
              assertThat(p.sku()).matches("^[A-Z0-9][A-Z0-9-]{2,39}$");
              assertThat(p.price().scale()).isEqualTo(2);
              assertThat(p.stock()).isNotNegative();
              assertThat(p.description()).isNotBlank();
            });
    assertThat(products).filteredOn(p -> !p.active()).hasSize(1);
  }

  @Test
  void keepsWhatTheDrillsRelyOn() {
    var products = DemoCatalog.generate(new Random());

    assertThat(products)
        .extracting(DemoCatalog.DemoProduct::sku)
        .contains("SKU-KEYBOARD", "SKU-MOUSE", "SKU-MONITOR", "SKU-CABLE");
    assertThat(products)
        .filteredOn(p -> p.sku().equals("SKU-CABLE"))
        .singleElement()
        .satisfies(p -> assertThat(p.stock()).isGreaterThanOrEqualTo(300));
    assertThat(products).filteredOn(p -> p.active() && p.stock() <= 3).isNotEmpty();
  }

  @Test
  void aSeedMakesTheCatalogRepeatable() {
    assertThat(DemoCatalog.generate(new Random(42)))
        .isEqualTo(DemoCatalog.generate(new Random(42)));
  }
}
