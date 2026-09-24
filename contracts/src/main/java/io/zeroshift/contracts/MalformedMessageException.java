package io.zeroshift.contracts;

/**
 * A record no retry can fix: unreadable JSON, an unknown type or a schema version newer than this
 * consumer understands. Consumers send it straight to the dead-letter topic.
 */
public final class MalformedMessageException extends RuntimeException {
  public MalformedMessageException(String message, Throwable cause) {
    super(message, cause);
  }

  public MalformedMessageException(String message) {
    super(message);
  }
}
