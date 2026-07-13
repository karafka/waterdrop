# frozen_string_literal: true

# Canary for librdkafka#4849. This spec exists to tell us when we can DELETE code, not to guard a
# feature. It has two halves, and each half fails for a different, useful reason:
#
#   1. WITHOUT the mitigation, the defect MUST still reproduce.
#      If it stops reproducing, librdkafka (or the broker) most likely fixed it - at which point
#      `wait_timeout_on_transaction_abort`, its documentation and this spec can all be retired.
#      That is the whole point of this file: it is a retirement alarm.
#
#   2. WITH the mitigation, the defect MUST NOT reproduce.
#      If it starts reproducing again, the mitigation has stopped working.
#
# Reproducing #4849 reliably is subtler than it looks. On abort, librdkafka only sends `EndTxn` if an
# `AddPartitionsToTxn` request has already been SENT (it gates on `txn_req_cnt`, bumped on send).
# That gives three outcomes:
#
#   * abort before the request is sent      -> `EndTxn` is skipped entirely       -> no error
#   * abort after SENT but before ANSWERED  -> `EndTxn` races the registration    -> FATAL (the bug)
#   * abort after the response landed       -> the transaction is ongoing         -> no error
#
# So aborting as fast as possible actually MISSES the bug - it lands in the first case. The window is
# a narrow band a few milliseconds wide, and its position depends on the coordinator round-trip, so
# it moves between machines. We therefore sweep a range of produce->abort delays rather than betting
# on one, and we loop until the fatal shows up (it is probabilistic - a few percent per abort at the
# peak) instead of asserting that any single abort must fail.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

PRODUCER_COUNT = 8
# Produce->abort delays (seconds) to sweep. 0 is deliberately excluded: it reliably misses the bug.
# The spread covers a moving window - on a slower coordinator the band sits further right.
DELAYS = [0.001, 0.002, 0.003, 0.005, 0.008, 0.013, 0.02, 0.03, 0.05].freeze
# Budget for phase 1. We stop the moment the defect shows up, so a healthy (still-broken) librdkafka
# exits in a second or two. The budget only gets spent in full when the defect is GONE - and it is
# sized so that a false "it is fixed" is implausible: at the ~5% peak rate seeing zero across this
# many aborts is essentially impossible, and even at a tenth of that it stays negligible.
MAX_ABORTS = 2_400
MAX_SECONDS = 120
# Phase 2 does not need to be as large: we only need enough aborts that, had the mitigation not
# worked, we would very likely have seen a fatal.
MITIGATED_ABORTS = 480
# ...and at least this many must actually run, otherwise "0 fatals" proves nothing and we would be
# passing on a sample too small to have caught a broken mitigation.
MIN_MITIGATED_ABORTS = 240

topic = generate_topic("tx-abort-canary")
create_topic(topic)

# Runs commit-then-abort cycles, sweeping the delay range, and returns how many INVALID_TXN_STATE
# fatals were observed. Stops early once `stop_after_first` is satisfied.
#
# @param abort_wait [Integer] `wait_timeout_on_transaction_abort` for the producers (0 disables it)
# @param max_aborts [Integer] how many aborts to run at most
# @param stop_after_first [Boolean] stop as soon as the defect is observed
# @return [Hash] `:fatals`, `:aborts` and `:timed_out` - the last one telling us whether the clock,
#   rather than the abort budget, is what ended the run. Without it we could not tell "the defect is
#   gone" from "this machine was too slow to spend the budget", and would raise a false alarm.
def run_cycles(topic, abort_wait:, max_aborts:, stop_after_first:)
  mutex = Mutex.new
  fatals = 0
  aborts = 0
  timed_out = false
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + MAX_SECONDS

  producers = Array.new(PRODUCER_COUNT) do
    WaterDrop::Producer.new do |config|
      config.kafka = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "transactional.id": generate_topic("tx-abort-canary-id"),
        "transaction.timeout.ms": 30_000,
        "message.timeout.ms": 30_000
      }
      config.max_wait_timeout = 30_000
      config.wait_backoff_on_transaction_fatal_error = 100
      config.wait_timeout_on_transaction_abort = abort_wait
      config.logger = Logger.new(File::NULL)
    end
  end

  # Warm up: connect and run init_transactions, so we race only the abort, not the first connect.
  producers.each_with_index do |producer, idx|
    producer.transaction { producer.produce_async(topic: topic, payload: "warm-#{idx}") }
  end

  per_producer = (max_aborts.to_f / PRODUCER_COUNT).ceil

  threads = producers.each_with_index.map do |producer, pidx|
    Thread.new do
      per_producer.times do |cycle|
        break if stop_after_first && mutex.synchronize { fatals.positive? }

        if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          mutex.synchronize { timed_out = true }

          break
        end

        # The race needs a producer that JUST committed a previous transaction.
        producer.transaction do
          producer.produce_async(topic: topic, payload: "commit-#{pidx}-#{cycle}")
        end

        begin
          producer.transaction do
            producer.produce_async(topic: topic, payload: "abort-#{pidx}-#{cycle}")

            # Land inside the window: after AddPartitionsToTxn is sent, before it is answered.
            sleep(DELAYS[cycle % DELAYS.size])

            mutex.synchronize { aborts += 1 }

            raise WaterDrop::AbortTransaction
          end
        rescue Rdkafka::RdkafkaError => e
          mutex.synchronize { fatals += 1 if e.code == :invalid_txn_state }
        end
      end
    rescue => e
      warn "producer #{pidx} died: #{e.class}: #{e.message}"
    end
  end

  threads.each(&:join)
  producers.each(&:close)

  { fatals: fatals, aborts: aborts, timed_out: timed_out }
end

# Phase 1: the defect must still be there. This is the retirement alarm.
unmitigated = run_cycles(
  topic,
  abort_wait: 0,
  max_aborts: MAX_ABORTS,
  stop_after_first: true
)

puts "unmitigated: #{unmitigated[:fatals]} fatal(s) in #{unmitigated[:aborts]} aborts"

# The clock ran out before we could spend the abort budget, so we simply do not know whether the
# defect is gone - we never gave it a fair chance. Saying "librdkafka fixed it" here would be a lie
# that reads as an instruction to delete a mitigation that may well still be needed.
if unmitigated[:fatals].zero? && unmitigated[:timed_out]
  warn <<~MSG
    FAIL (inconclusive): only #{unmitigated[:aborts]} of the #{MAX_ABORTS} budgeted aborts ran before
    the #{MAX_SECONDS}s deadline, so we never gave librdkafka#4849 a fair chance to show up. This says
    NOTHING about whether the defect is fixed - do not retire the mitigation on the back of it.

    The machine is likely just loaded or slow. Re-run, or raise MAX_SECONDS.
  MSG

  exit!(1)
end

if unmitigated[:fatals].zero?
  warn <<~MSG
    FAIL: librdkafka#4849 did NOT reproduce in #{unmitigated[:aborts]} aborts (the full budget)
    across the whole delay sweep (#{DELAYS.map { |d| "#{(d * 1000).round}ms" }.join(", ")}).

    This spec is a canary, so this failure is GOOD NEWS in all likelihood: the defect appears to be
    fixed upstream. Confirm against the librdkafka changelog, then retire the workaround:
      - `wait_timeout_on_transaction_abort` (config + contract + docs)
      - `#transactional_await_first_delivery` and the first-handle tracking in `#produce_to_client`
      - the `tx-abort-(race|canary)-id` allowances in `bin/verify_kafka_warnings`
      - the `transactional_abort_after_commit_race` spec, and this one

    If instead the cluster is simply too fast/slow for the window to land, widen DELAYS.
  MSG

  exit!(1)
end

# Phase 2: with the mitigation on, the same cycles must produce no fatal at all.
mitigated = run_cycles(
  topic,
  abort_wait: 5_000,
  max_aborts: MITIGATED_ABORTS,
  stop_after_first: false
)

puts "mitigated:   #{mitigated[:fatals]} fatal(s) in #{mitigated[:aborts]} aborts"

unless mitigated[:fatals].zero?
  warn <<~MSG
    FAIL: librdkafka#4849 still fired #{mitigated[:fatals]} time(s) in #{mitigated[:aborts]} aborts
    WITH `wait_timeout_on_transaction_abort` enabled. Waiting for the first delivery is supposed to
    prove the coordinator registered the transaction, so the abort's EndTxn can no longer race it.
    The mitigation is no longer sufficient - do not trust it.
  MSG

  exit!(1)
end

# "0 fatals" only means something if we ran enough aborts that the defect would very likely have
# shown up had the mitigation not worked. Without this floor a deadline-truncated phase 2 would pass
# on a handful of aborts and silently claim the mitigation still works.
if mitigated[:aborts] < MIN_MITIGATED_ABORTS
  warn <<~MSG
    FAIL (inconclusive): phase 2 only ran #{mitigated[:aborts]} aborts (needs at least
    #{MIN_MITIGATED_ABORTS}), so "0 fatals" does not yet demonstrate the mitigation works - there
    were simply too few attempts. The deadline most likely cut the run short; re-run, or raise
    MAX_SECONDS.
  MSG

  exit!(1)
end

puts "PASS: #4849 still reproduces unmitigated and is fully suppressed by the abort delivery wait"
exit(0)
