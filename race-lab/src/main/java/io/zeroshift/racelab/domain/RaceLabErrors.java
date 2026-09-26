package io.zeroshift.racelab.domain;

/** The lab's refusals. The web layer maps each to its HTTP status and stable error code. */
public final class RaceLabErrors {
  private RaceLabErrors() {}

  public static class RaceLabException extends RuntimeException {
    RaceLabException(String message) {
      super(message);
    }
  }

  public static final class ExperimentNotFound extends RaceLabException {
    public ExperimentNotFound(String id) {
      super("No experiment named '" + id + "'");
    }
  }

  public static final class RunNotFound extends RaceLabException {
    public RunNotFound(long id) {
      super("No run #" + id);
    }
  }

  /** The configuration breaks the experiment's own rules: an unsupported mode, a bound. */
  public static final class InvalidConfig extends RaceLabException {
    public InvalidConfig(String message) {
      super(message);
    }
  }

  /** One run at a time: concurrent runs would contend with each other and blur the lesson. */
  public static final class LabBusy extends RaceLabException {
    private final long runningId;

    public LabBusy(long runningId) {
      super("Run #" + runningId + " is still running; start the next one when it finishes");
      this.runningId = runningId;
    }

    public long runningId() {
      return runningId;
    }
  }

  public static final class RunAlreadyStarted extends RaceLabException {
    public RunAlreadyStarted(long id, Run.Status status) {
      super("Run #" + id + " is " + status.name().toLowerCase() + "; create a new run instead");
    }
  }
}
