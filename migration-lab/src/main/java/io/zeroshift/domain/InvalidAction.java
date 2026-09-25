package io.zeroshift.domain;

public final class InvalidAction extends MigrationException {
  public InvalidAction(String message) {
    super(message);
  }
}
