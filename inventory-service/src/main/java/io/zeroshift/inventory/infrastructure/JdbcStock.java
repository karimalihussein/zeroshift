package io.zeroshift.inventory.infrastructure;

import io.zeroshift.contracts.InventoryCommand.StockLine;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.inventory.application.Stock;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.core.type.TypeReference;

public final class JdbcStock implements Stock {
  private static final TypeReference<List<StockLine>> LINES = new TypeReference<>() {};
  private final JdbcTemplate jdbc;

  public JdbcStock(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<Item> find(String sku) {
    return jdbc
        .query(
            "SELECT sku,on_hand,reserved,version FROM stock WHERE sku=?",
            (r, n) -> new Item(r.getString(1), r.getInt(2), r.getInt(3), r.getInt(4)),
            sku)
        .stream()
        .findFirst();
  }

  @Override
  public boolean adjust(Item item, int delta) {
    return jdbc.update(
            "UPDATE stock SET reserved=reserved+?,version=version+1 WHERE sku=? AND version=?",
            delta,
            item.sku(),
            item.version())
        == 1;
  }

  @Override
  public Optional<Reservation> reservation(UUID orderId) {
    return jdbc
        .query(
            "SELECT order_id,reservation_id,status,lines::text,reason FROM reservation WHERE order_id=?",
            (r, n) ->
                new Reservation(
                    r.getObject(1, UUID.class),
                    r.getObject(2, UUID.class),
                    Status.valueOf(r.getString(3)),
                    MessageCodec.json().readValue(r.getString(4), LINES),
                    r.getString(5)),
            orderId)
        .stream()
        .findFirst();
  }

  @Override
  public void save(Reservation reservation) {
    jdbc.update(
        "INSERT INTO reservation(order_id,reservation_id,status,lines,reason) VALUES(?,?,?,?::jsonb,?)"
            + " ON CONFLICT(order_id) DO UPDATE SET status=EXCLUDED.status,reason=EXCLUDED.reason,"
            + "updated_at=clock_timestamp()",
        reservation.orderId(),
        reservation.reservationId(),
        reservation.status().name(),
        MessageCodec.json().writeValueAsString(reservation.lines()),
        reservation.reason());
  }

  public List<Map<String, Object>> levels() {
    return jdbc.queryForList(
        "SELECT sku,name,on_hand,reserved,on_hand-reserved AS available,version FROM stock ORDER BY sku");
  }
}
