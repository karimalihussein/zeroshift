package io.zeroshift.eventlab;

import java.util.ArrayList;
import java.util.List;
import java.util.random.RandomGenerator;
import org.springframework.stereotype.Component;

/**
 * The seeded customers the control plane's own orders are placed for (load, history and client
 * retry labs). Orders need a real customer of order-service; the list is read once and kept, since
 * customers are seed data that does not change while the lab runs.
 */
@Component
public class LabCustomers {
  public record Customer(String id, String name) {}

  private final LabServices services;
  private volatile List<Customer> known = List.of();

  public LabCustomers(LabServices services) {
    this.services = services;
  }

  public List<Customer> all() {
    if (known.isEmpty()) {
      var found = new ArrayList<Customer>();
      for (var c :
          LabServices.data(services.tryGetAnyReplica("order-service", "/customers?limit=100")))
        found.add(new Customer(c.path("id").asString(), c.path("name").asString()));
      known = List.copyOf(found);
    }
    return known;
  }

  /** A random seeded customer; fails clearly when order-service has none (demo data off). */
  public Customer any(RandomGenerator random) {
    var all = all();
    if (all.isEmpty())
      throw new LabServices.ActionFailed(
          "order-service has no customers: start it with COMMERCE_DEMO_DATA=true");
    return all.get(random.nextInt(all.size()));
  }
}
