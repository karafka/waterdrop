# frozen_string_literal: true

# Integration test reproducing the lock-order-inversion deadlock between a transactional
# single-message dispatch and #close.
#
# For a transactional producer, Producer#produce (1) increments @operations_in_progress under
# @operating_mutex and releases it, (2) passes ensure_active!, and only later (3) wraps the dispatch
# in `transaction { ... }`, which acquires @transaction_mutex. #close acquires the locks in the
# OPPOSITE order: @transaction_mutex -> @operating_mutex -> then busy-waits until the operations
# counter reaches zero. If #close grabs @transaction_mutex in the window where a produce has already
# incremented the counter but not yet acquired @transaction_mutex, the two deadlock permanently:
# produce blocks forever on @transaction_mutex (held by close), so it never decrements the counter,
# and close waits forever for the counter to drain. There is no timeout on the close wait loop.
#
# To force that exact window deterministically WITHOUT stubbing any WaterDrop internals, we pass a
# custom message object whose (one-shot) #dup blocks. Producer#produce calls message.dup exactly
# once - after the operation is counted and ensure_active! has passed, and before @transaction_mutex
# is taken (it is the dup that copies the message to stringify a symbol topic / attach topic_config).
# Pausing there reproduces the precise interleaving with real threads and a real transactional
# producer.

require "waterdrop"
require "logger"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Message that pauses Producer#produce inside the deadlock window. Its #dup (called once by produce,
# after the op is counted and before @transaction_mutex is taken) signals the test and then blocks
# until released. It is plain user input - no WaterDrop internals are stubbed.
class PausingMessage < Hash
  def arm(reached, resume)
    @reached = reached
    @resume = resume
    @armed = true
    self
  end

  def dup
    if @armed
      @armed = false
      @reached << true
      @resume.pop
    end

    super
  end
end

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": generate_topic("wd03-tx")
  }
  config.max_wait_timeout = 10_000
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

topic = generate_topic("wd03-close-deadlock")

# Warm up: connect, run init_transactions, create the topic. After this the producer is fully ready,
# so the test exercises only the produce-vs-close race.
producer.produce_sync(topic: topic, payload: "warmup")

reached = Queue.new
resume = Queue.new

message = PausingMessage.new
# Symbol topic makes Producer#produce take the dup branch - that dup is our pause point.
message[:topic] = topic.to_sym
message[:payload] = "racy"
message.arm(reached, resume)

# Thread A: one transactional dispatch. It parks inside #produce at message.dup, having already
# incremented the operations counter and passed ensure_active!, but before taking @transaction_mutex.
producer_thread = Thread.new { producer.produce_async(message) }

# Wait until A is parked in the window.
reached.pop

# Thread B: close races in now. It takes @transaction_mutex + @operating_mutex and starts draining
# the operations counter, which is already 1 because of A.
close_thread = Thread.new { producer.close }

# Give close time to take @transaction_mutex and enter its counter-drain wait loop.
sleep(0.5)

# Release A. It now proceeds to `transaction { ... }` -> @transaction_mutex.synchronize. With the
# bug that mutex is held by close (which is itself waiting for A's operation to finish) => permanent
# deadlock. With the fix, the transactional dispatch already owns @transaction_mutex (taken before
# the operation is counted), so close simply waits for A to finish and then closes cleanly.
resume << true

TIMEOUT = 15

produced_ok = producer_thread.join(TIMEOUT)
closed_ok = produced_ok ? close_thread.join(TIMEOUT) : false

if produced_ok && closed_ok
  puts "PASS: transactional produce racing close completed without deadlock"
  exit(0)
else
  warn "FAIL: deadlock - produce thread #{produced_ok ? "finished" : "STUCK"}, " \
       "close thread #{closed_ok ? "finished" : "STUCK"}"
  # The threads are wedged on mutexes; force-exit so we do not hang on finalizers trying to close
  # the already-deadlocked producer again.
  exit!(1)
end
