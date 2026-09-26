package io.zeroshift.inventory.infrastructure;

import static io.zeroshift.inventory.db.Tables.PRODUCT;

import io.zeroshift.contracts.Money;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.random.RandomGenerator;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The demo catalog, inserted when {@code commerce.demo-data} is on and there are no products yet.
 * Names, prices and SKUs are fixed; stock is drawn from each template's range ({@code
 * commerce.demo-seed} makes the draw repeatable). Ids derive from the SKU, so they survive a
 * rebuild.
 */
public final class DemoCatalog {
  private static final Logger log = LoggerFactory.getLogger(DemoCatalog.class);

  record Template(
      String sku,
      String name,
      String description,
      String price,
      int minStock,
      int maxStock,
      boolean active) {
    Template(String sku, String name, String description, String price, int min, int max) {
      this(sku, name, description, price, min, max, true);
    }
  }

  record DemoProduct(
      UUID id,
      String sku,
      String name,
      String description,
      BigDecimal price,
      int stock,
      boolean active) {}

  static final List<Template> TEMPLATES =
      List.of(
          // Drills and the race lab name these four.
          new Template(
              "SKU-KEYBOARD",
              "Mechanical keyboard",
              "Tenkeyless board with hot-swappable brown switches and PBT keycaps.",
              "89.99",
              20,
              40),
          new Template(
              "SKU-MOUSE",
              "Wireless mouse",
              "Ergonomic 2.4 GHz and Bluetooth mouse with a 70-day battery.",
              "29.99",
              80,
              150),
          new Template(
              "SKU-MONITOR",
              "27\" 4K monitor",
              "IPS panel with USB-C power delivery and a height-adjustable stand.",
              "349.00",
              1,
              3),
          new Template(
              "SKU-CABLE",
              "USB-C cable, 2 m",
              "Braided USB 3.2 cable rated for 100 W charging and 10 Gbps data.",
              "12.99",
              400,
              600),
          new Template(
              "SKU-WEBCAM",
              "1080p webcam",
              "Full HD webcam with autofocus, a privacy shutter and dual microphones.",
              "59.00",
              30,
              70),
          new Template(
              "SKU-HEADSET",
              "Noise-cancelling headset",
              "Over-ear wireless headset with a boom mic for long calls.",
              "149.00",
              15,
              35),
          new Template(
              "SKU-DOCK",
              "USB-C docking station",
              "Two HDMI outputs, gigabit Ethernet and four USB ports from one cable.",
              "189.00",
              10,
              25),
          new Template(
              "SKU-SSD-1TB",
              "Portable SSD, 1 TB",
              "Pocket-sized drive with 1,050 MB/s reads, in an aluminium shell.",
              "109.99",
              25,
              60),
          new Template(
              "SKU-HUB",
              "4-port USB hub",
              "Bus-powered USB 3.0 hub with individual power switches.",
              "19.99",
              60,
              120),
          new Template(
              "SKU-CHARGER",
              "65 W GaN charger",
              "Compact two-port charger that powers a laptop and a phone at once.",
              "44.99",
              50,
              100),
          new Template(
              "SKU-SPEAKER",
              "Bluetooth speaker",
              "Water-resistant speaker with 12 hours of playback.",
              "69.00",
              20,
              45),
          new Template(
              "SKU-TABLET-STAND",
              "Aluminium tablet stand",
              "Adjustable stand for tablets and phones up to 13 inches.",
              "24.50",
              40,
              90),
          new Template(
              "SKU-LAPTOP-STAND",
              "Laptop riser",
              "Raises a laptop screen to eye level; folds flat for travel.",
              "39.00",
              30,
              70),
          new Template(
              "SKU-MIC",
              "USB condenser microphone",
              "Cardioid microphone with a headphone jack and mute button.",
              "99.00",
              1,
              2),
          new Template(
              "SKU-DESK-LAMP",
              "LED desk lamp",
              "Dimmable lamp with adjustable colour temperature and a USB port.",
              "34.99",
              25,
              60),
          new Template(
              "SKU-DESK-MAT",
              "Felt desk mat",
              "Large wool-felt mat that covers keyboard and mouse.",
              "27.00",
              40,
              100),
          new Template(
              "SKU-CHAIR",
              "Ergonomic office chair",
              "Mesh back, adjustable lumbar support and 4D armrests.",
              "329.00",
              4,
              12),
          new Template(
              "SKU-DESK",
              "Standing desk",
              "Electric sit-stand desk with memory presets, 140 × 70 cm.",
              "499.00",
              3,
              8),
          new Template(
              "SKU-NOTEBOOK",
              "Dotted notebook, A5",
              "192 numbered pages of 100 gsm paper with a lay-flat binding.",
              "14.00",
              100,
              250),
          new Template(
              "SKU-PENS",
              "Gel pens, pack of 10",
              "Quick-drying 0.5 mm gel pens in black.",
              "9.49",
              120,
              300),
          new Template(
              "SKU-STAPLER",
              "Heavy-duty stapler",
              "Staples up to 60 sheets with little effort.",
              "22.00",
              30,
              60),
          new Template(
              "SKU-SHREDDER",
              "Micro-cut shredder",
              "Shreds 8 sheets at a time into security level P-4 particles.",
              "129.00",
              6,
              15),
          new Template(
              "SKU-ORGANIZER",
              "Desk organizer",
              "Bamboo organizer with drawers for pens, cards and cables.",
              "31.00",
              25,
              55),
          new Template(
              "SKU-WHITEBOARD",
              "Magnetic whiteboard",
              "90 × 60 cm board with an aluminium frame and marker tray.",
              "64.00",
              8,
              20),
          new Template(
              "SKU-MUG",
              "Insulated mug",
              "Double-walled steel mug that keeps coffee hot for hours.",
              "18.00",
              60,
              140),
          new Template(
              "SKU-KETTLE",
              "Electric kettle",
              "1.7 l kettle with temperature presets and keep-warm.",
              "54.99",
              15,
              40),
          new Template(
              "SKU-PLANT-POT",
              "Ceramic plant pot",
              "Matte ceramic pot with a drainage tray, 15 cm.",
              "16.50",
              30,
              80),
          new Template(
              "SKU-THROW",
              "Knitted throw blanket",
              "Soft cotton throw, 130 × 170 cm.",
              "42.00",
              20,
              50),
          new Template(
              "SKU-DIFFUSER",
              "Aroma diffuser",
              "Ultrasonic diffuser with a timer and soft light.",
              "29.00",
              25,
              60),
          new Template(
              "SKU-FAX",
              "Fax machine",
              "Plain-paper fax with a 20-page feeder. No longer sold.",
              "119.00",
              0,
              5,
              false));

  private final DSLContext db;
  private final RandomGenerator random;

  public DemoCatalog(DSLContext db, Long seed) {
    this.db = db;
    this.random = seed == null ? new Random() : new Random(seed);
  }

  /** Inserts the catalog if there are no products. Concurrent replicas insert each SKU once. */
  public void seedIfEmpty() {
    if (db.fetchExists(PRODUCT)) return;
    var products = generate(random);
    for (var p : products)
      db.insertInto(PRODUCT)
          .set(PRODUCT.ID, p.id())
          .set(PRODUCT.SKU, p.sku())
          .set(PRODUCT.NAME, p.name())
          .set(PRODUCT.DESCRIPTION, p.description())
          .set(PRODUCT.PRICE, p.price())
          .set(PRODUCT.STOCK, p.stock())
          .set(PRODUCT.ACTIVE, p.active())
          .onConflict(PRODUCT.SKU)
          .doNothing()
          .execute();
    log.info("Seeded the demo catalog: {} products", products.size());
  }

  static List<DemoProduct> generate(RandomGenerator random) {
    return TEMPLATES.stream()
        .map(
            t ->
                new DemoProduct(
                    UUID.nameUUIDFromBytes(("product:" + t.sku()).getBytes(StandardCharsets.UTF_8)),
                    t.sku(),
                    t.name(),
                    t.description(),
                    Money.exact(new BigDecimal(t.price())),
                    random.nextInt(t.minStock(), t.maxStock() + 1),
                    t.active()))
        .toList();
  }
}
