package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.CUSTOMER;
import static io.zeroshift.order.db.Tables.VOUCHER;

import java.math.BigDecimal;
import java.text.Normalizer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Customers and vouchers for the lab (commerce.demo-data, on in Compose, off in tests), inserted
 * once, when the customer table is empty. Names, emails and phones are randomized (reproducibly
 * with commerce.demo-seed); the vouchers are fixed, one per case worth showing. Orders are never
 * seeded: they are placed through POST /orders so that everything downstream is real.
 */
public class DemoData implements ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(DemoData.class);
  static final int CUSTOMERS = 40;

  private static final List<String> FIRST_NAMES =
      List.of(
          "Amara",
          "Liam",
          "Sofia",
          "Kenji",
          "Fatima",
          "Mateo",
          "Ingrid",
          "Arjun",
          "Chloé",
          "Kwame",
          "Olivia",
          "Diego",
          "Yuki",
          "Hannah",
          "Omar",
          "Priya",
          "Lucas",
          "Zanele",
          "Mei",
          "Noah",
          "Aisha",
          "Jonas",
          "Valentina",
          "Tariq",
          "Elena",
          "Samuel",
          "Leila",
          "Tomás",
          "Grace",
          "Ravi",
          "Freya",
          "Mohammed",
          "Isabella",
          "Chen",
          "Nadia",
          "Oliver",
          "Ana",
          "Daniel",
          "Sara",
          "Emeka");

  private static final List<String> LAST_NAMES =
      List.of(
          "Okafor",
          "Müller",
          "García",
          "Tanaka",
          "Haddad",
          "Rossi",
          "Johansson",
          "Patel",
          "Dubois",
          "Mensah",
          "Smith",
          "Hernández",
          "Nakamura",
          "Schmidt",
          "Khalil",
          "Sharma",
          "Silva",
          "Dlamini",
          "Wang",
          "Cohen",
          "Rahman",
          "Novak",
          "López",
          "Nguyen",
          "Kowalski",
          "Brown",
          "Fischer",
          "Costa",
          "Kim",
          "O'Brien",
          "Andersen",
          "Moreau",
          "Yilmaz",
          "Adeyemi",
          "Russo",
          "Ivanova",
          "Santos",
          "Murphy",
          "Chowdhury",
          "Lindqvist");

  private static final List<String> MAIL_DOMAINS =
      List.of("gmail.com", "outlook.com", "yahoo.com", "icloud.com", "proton.me", "fastmail.com");

  private static final List<String> AREA_CODES =
      List.of("212", "312", "415", "503", "617", "646", "702", "713", "206", "305", "404", "512");

  private final DSLContext db;
  private final TransactionOperations transactions;
  private final Random random;

  public DemoData(DSLContext db, TransactionOperations transactions, Long seed) {
    this.db = db;
    this.transactions = transactions;
    this.random = seed == null ? new Random() : new Random(seed);
  }

  @Override
  public void run(ApplicationArguments args) {
    transactions.executeWithoutResult(
        tx -> {
          // Both replicas start at once: the lock makes the second wait, then find the data.
          db.execute("LOCK TABLE customer, voucher IN SHARE ROW EXCLUSIVE MODE");
          if (db.fetchExists(CUSTOMER)) return;
          customers();
          vouchers();
          log.info("Seeded {} demo customers and the demo vouchers", CUSTOMERS);
        });
  }

  private void customers() {
    var names = new HashSet<String>();
    var emails = new HashSet<String>();
    while (names.size() < CUSTOMERS) {
      var first = pick(FIRST_NAMES);
      var last = pick(LAST_NAMES);
      if (!names.add(first + " " + last)) continue;
      var local = ascii(first) + "." + ascii(last);
      var email = local + "@" + pick(MAIL_DOMAINS);
      for (int n = 2; !emails.add(email); n++) email = local + n + "@" + pick(MAIL_DOMAINS);
      db.insertInto(CUSTOMER)
          .set(CUSTOMER.ID, UUID.randomUUID())
          .set(CUSTOMER.NAME, first + " " + last)
          .set(CUSTOMER.EMAIL, email)
          .set(CUSTOMER.PHONE, phone())
          .execute();
    }
  }

  /** One voucher per case: percentage, fixed with a minimum, capped, scarce, expired, off. */
  private void vouchers() {
    var now = OffsetDateTime.now(ZoneOffset.UTC);
    var monthAgo = now.minus(Duration.ofDays(30));
    voucher("WELCOME10", "PERCENTAGE", "10.00", "0.00", null, null, monthAgo, null, true);
    voucher("SAVE15", "FIXED", "15.00", "100.00", null, null, monthAgo, null, true);
    voucher("BIG20", "PERCENTAGE", "20.00", "0.00", "50.00", null, monthAgo, null, true);
    voucher("FLASH5", "FIXED", "5.00", "0.00", null, 5, monthAgo, now.plusDays(7), true);
    voucher("VIP50", "FIXED", "50.00", "250.00", null, 100, monthAgo, null, true);
    voucher(
        "SPRING25",
        "PERCENTAGE",
        "25.00",
        "200.00",
        "75.00",
        null,
        monthAgo,
        now.plusDays(60),
        true);
    voucher(
        "SUMMER24",
        "PERCENTAGE",
        "15.00",
        "0.00",
        null,
        null,
        now.minusDays(400),
        now.minusDays(300),
        true);
    voucher("LAUNCH30", "PERCENTAGE", "30.00", "0.00", "40.00", null, monthAgo, null, false);
  }

  private void voucher(
      String code,
      String type,
      String value,
      String minimum,
      String maximum,
      Integer usageLimit,
      OffsetDateTime validFrom,
      OffsetDateTime validUntil,
      boolean active) {
    db.insertInto(VOUCHER)
        .set(VOUCHER.ID, UUID.randomUUID())
        .set(VOUCHER.CODE, code)
        .set(VOUCHER.DISCOUNT_TYPE, type)
        .set(VOUCHER.VALUE, new BigDecimal(value))
        .set(VOUCHER.MINIMUM_AMOUNT, new BigDecimal(minimum))
        .set(VOUCHER.MAXIMUM_DISCOUNT, maximum == null ? null : new BigDecimal(maximum))
        .set(VOUCHER.USAGE_LIMIT, usageLimit)
        .set(VOUCHER.VALID_FROM, validFrom)
        .set(VOUCHER.VALID_UNTIL, validUntil)
        .set(VOUCHER.ACTIVE, active)
        .execute();
  }

  /** A US number in a real area code (exchanges start at 2), as people write it. */
  private String phone() {
    return "+1 (%s) %03d-%04d"
        .formatted(pick(AREA_CODES), 200 + random.nextInt(800), random.nextInt(10_000));
  }

  private <T> T pick(List<T> values) {
    return values.get(random.nextInt(values.size()));
  }

  /** "Chloé O'Brien" → "chloe", "obrien": an email local part people actually have. */
  static String ascii(String name) {
    return Normalizer.normalize(name, Normalizer.Form.NFD)
        .replaceAll("\\p{M}", "")
        .replaceAll("[^A-Za-z]", "")
        .toLowerCase(Locale.ROOT);
  }
}
