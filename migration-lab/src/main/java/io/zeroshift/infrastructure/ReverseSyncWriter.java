package io.zeroshift.infrastructure;

/**
 * How SQL Server recognizes reverse sync. Its session context lets it past the source fence, and
 * its Change Tracking context marks its writes so any other post-cutover write stands out.
 */
final class ReverseSyncWriter {
  static final String SESSION_KEY = "zeroshift.writer";
  static final String SESSION_VALUE = "reverse-sync";

  /** ASCII "ZSREVSYNC". */
  static final String CHANGE_CONTEXT = "0x5A5352455653594E43";

  private ReverseSyncWriter() {}
}
