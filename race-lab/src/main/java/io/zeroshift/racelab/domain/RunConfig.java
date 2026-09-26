package io.zeroshift.racelab.domain;

/**
 * What to run: the experiment, how requests protect the shared data, and the race's shape.
 *
 * @param requests concurrent requests, each its own transaction on its own connection
 * @param initialValue the starting value of the shared data (stock, balance, version…)
 * @param delayMs application work between reading and writing: the race window
 * @param maxRetries how often a request aborted by a conflict starts over
 */
public record RunConfig(
    String experiment,
    Mode mode,
    Isolation isolation,
    int requests,
    int initialValue,
    int delayMs,
    Interleaving interleaving,
    int maxRetries) {}
