package io.zeroshift.domain;

public final class CaptureExpired extends MigrationException {
  public CaptureExpired(long version) {
    super(
        "Change Tracking retention expired at version "
            + version
            + ". Reset and take a new snapshot.");
  }
}
