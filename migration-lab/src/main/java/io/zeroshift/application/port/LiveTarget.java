package io.zeroshift.application.port;

import io.zeroshift.domain.OrderRecord;
import java.util.List;
import java.util.Optional;

public interface LiveTarget {
  Optional<OrderRecord> order(long id);

  List<Long> candidates();
}
