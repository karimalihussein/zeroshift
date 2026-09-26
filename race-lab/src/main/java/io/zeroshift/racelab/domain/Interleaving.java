package io.zeroshift.racelab.domain;

/**
 * Whether the lab forces the dangerous interleaving or leaves timing to chance.
 *
 * <p>CONTROLLED makes requests take turns at named points (read, then write) and wait at barriers
 * until every other request has arrived, finished, or is blocked inside PostgreSQL. The SQL, locks
 * and isolation are unchanged: only the moment each request issues its next statement is chosen.
 * NATURAL starts every request at once and lets the scheduler and the database decide.
 */
public enum Interleaving {
  CONTROLLED,
  NATURAL
}
