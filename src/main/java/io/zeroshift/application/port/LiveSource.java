package io.zeroshift.application.port;

import io.zeroshift.domain.*;
import java.util.Optional;

public interface LiveSource {
  Optional<OrderRecord> order(long id);

  Optional<LiveExperiment> latest(long orderId);

  LiveExperiment insert(OrderEdit edit);

  LiveExperiment update(long id, OrderEdit edit);

  LiveExperiment delete(long id);
}
