package io.zeroshift.shipping;

import io.zeroshift.platform.web.ApiException;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import java.util.Locale;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** The ordering lab's HTTP levers: publish carrier scans, repartition, replay, guard. */
@RestController
@RequestMapping("/lab")
public class CarrierLab {
  public record TrackingView(
      boolean guard,
      List<Tracking.Parcel> parcels,
      List<Tracking.Scan> scans,
      int partitions,
      String slow) {}

  public record Replayed(int replayedPartitions) {}

  public record Guard(boolean guard) {}

  private final Carrier carrier;
  private final Tracking tracking;

  public CarrierLab(Carrier carrier, Tracking tracking) {
    this.carrier = carrier;
    this.tracking = tracking;
  }

  /**
   * Publishes scans {@code fromSeq..toSeq} for the newest {@code parcels} shipped parcels. Starting
   * at scan 1 begins a fresh trip: those parcels' tracking is forgotten first.
   */
  @PostMapping("/carrier/scans")
  public Carrier.ScanRun scan(
      @RequestParam(defaultValue = "tracking") String keying,
      @RequestParam(defaultValue = "4") @Min(1) @Max(10) int parcels,
      @RequestParam(defaultValue = "1") @Min(1) @Max(4) int fromSeq,
      @RequestParam(defaultValue = "4") @Min(1) @Max(4) int toSeq)
      throws Exception {
    return carrier.scan(keying(keying), parcels, fromSeq, toSeq);
  }

  /** Adds partitions. Kafka can only add them: existing keys may now hash to a different one. */
  @PostMapping("/carrier/partitions")
  public Carrier.Partitions addPartitions(@RequestParam @Min(1) @Max(24) int count)
      throws Exception {
    return carrier.addPartitions(count);
  }

  /** Recovery from repartitioning: the topic is recreated with 3 partitions. */
  @PostMapping("/carrier/reset")
  public Carrier.Partitions resetTopic() throws Exception {
    return carrier.recreateTopic();
  }

  /** Replays every scan still on the topic into an empty tracking projection. */
  @PostMapping("/tracking/replay")
  public Replayed replay() throws Exception {
    return new Replayed(carrier.replay());
  }

  @PutMapping("/tracking/guard")
  public Guard guard(@RequestParam boolean on) {
    tracking.guard(on);
    return new Guard(on);
  }

  @GetMapping("/tracking")
  public TrackingView view() throws Exception {
    return new TrackingView(
        tracking.guarded(),
        tracking.parcels(12),
        tracking.scans(80),
        carrier.partitionCount(),
        carrier.slowFault());
  }

  private static Carrier.Keying keying(String value) {
    try {
      return Carrier.Keying.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new ApiException(
          HttpStatus.BAD_REQUEST,
          "UNKNOWN_KEYING",
          "Keying is tracking, scan or hub, not " + value);
    }
  }
}
