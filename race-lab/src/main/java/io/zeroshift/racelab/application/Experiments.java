package io.zeroshift.racelab.application;

import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.RaceLabErrors;
import java.util.Comparator;
import java.util.List;

/** The lab's experiments, in teaching order. */
public class Experiments {
  private final List<Experiment> all;

  public Experiments(List<Experiment> experiments) {
    all = experiments.stream().sorted(Comparator.comparingInt(e -> e.info().number())).toList();
  }

  public List<ExperimentInfo> catalog() {
    return all.stream().map(Experiment::info).toList();
  }

  public Experiment get(String id) {
    return all.stream()
        .filter(e -> e.info().id().equals(id))
        .findFirst()
        .orElseThrow(() -> new RaceLabErrors.ExperimentNotFound(id));
  }
}
