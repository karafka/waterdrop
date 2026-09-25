# frozen_string_literal: true

describe WaterDrop::Producer::Buffer do
  before do
    @producer = build(:producer)
    @invalid_error = WaterDrop::Errors::MessageInvalidError
  end

  after do
    @producer.purge
    @producer.close
  end

  # Drives the race this guards against: a producer that is closed after #buffer/#buffer_many pass
  # their liveness check but before the append acquires @buffer_mutex. Holding the mutex parks the
  # buffering thread exactly in that window, so the close it would otherwise race with is observed
  # deterministically rather than by timing luck.
  def buffer_racing_close(producer)
    buffer_mutex = producer.instance_variable_get(:@buffer_mutex)
    status = producer.instance_variable_get(:@status)
    error = nil

    buffer_mutex.lock

    thread = Thread.new do
      yield
    rescue WaterDrop::Errors::ProducerClosedError => e
      error = e
    end

    # Parked on @buffer_mutex means the liveness check above it has already passed
    wait_until { thread.status == "sleep" }

    # What `close` reaches before its final flush takes the same mutex
    status.closing!
    status.closed!

    buffer_mutex.unlock
    thread.join

    error
  end

  describe "#buffer" do
    context "when producer is closed" do
      before { @producer.close }

      it do
        @message = build(:valid_message)
        assert_raises(WaterDrop::Errors::ProducerClosedError) { @producer.buffer(@message) }
      end
    end

    context "when the producer is closed between the liveness check and the append" do
      it "expect to raise instead of stranding the message in a closed producer" do
        @message = build(:valid_message)

        error = buffer_racing_close(@producer) { @producer.buffer(@message) }

        assert_instance_of(WaterDrop::Errors::ProducerClosedError, error)
        assert_empty(@producer.instance_variable_get(:@messages))
      end
    end

    context "when message is invalid" do
      before do
        @message = build(:invalid_message)
      end

      it { @producer.buffer(@message) }

      it "expect to raise on attempt to flush" do
        @producer.buffer(@message)
        assert_raises(@invalid_error) { @producer.flush_async }
      end
    end

    context "when message is valid" do
      before do
        @message = build(:valid_message)
      end

      it { assert_includes(@producer.buffer(@message), @message) }
    end

    context "with middleware" do
      before do
        @message = build(:valid_message)

        @middleware = lambda do |message|
          message[:payload] += "test "
          message
        end

        @producer.middleware.append(@middleware)
      end

      it "expect to run middleware only once during the flow" do
        @producer.buffer(@message)
        @producer.flush_async

        assert_equal(1, @message[:payload].scan("test").size)
      end
    end
  end

  describe "#buffer_many" do
    context "when the producer is closed between the liveness check and the append" do
      it "expect to raise instead of stranding the messages in a closed producer" do
        @messages = [build(:valid_message)]

        error = buffer_racing_close(@producer) { @producer.buffer_many(@messages) }

        assert_instance_of(WaterDrop::Errors::ProducerClosedError, error)
        assert_empty(@producer.instance_variable_get(:@messages))
      end
    end

    context "when producer is closed" do
      before { @producer.close }

      it do
        @messages = [build(:valid_message)]
        assert_raises(WaterDrop::Errors::ProducerClosedError) { @producer.buffer_many(@messages) }
      end
    end

    context "when we have several invalid messages" do
      before do
        @messages = Array.new(10) { build(:invalid_message) }
      end

      it { @producer.buffer_many(@messages) }

      it "expect to validate on flush" do
        @producer.buffer_many(@messages)
        assert_raises(@invalid_error) { @producer.flush_async }
      end
    end

    context "when the last message out of a batch is invalid" do
      before do
        @messages = [build(:valid_message), build(:invalid_message)]
      end

      it { @producer.buffer_many(@messages) }

      it "expect to validate on flush" do
        @producer.buffer_many(@messages)
        assert_raises(@invalid_error) { @producer.flush_async }
      end
    end

    context "when we have several valid messages" do
      before do
        @messages = Array.new(10) { build(:valid_message) }
      end

      it "expect all the results to be buffered" do
        assert_equal(@messages, @producer.buffer_many(@messages))
      end
    end

    context "with middleware" do
      before do
        @message = build(:valid_message)

        @middleware = lambda do |message|
          message[:payload] += "test "
          message
        end

        @producer.middleware.append(@middleware)
      end

      it "expect to run middleware only once during the flow" do
        @producer.buffer_many([@message])
        @producer.flush_async

        assert_equal(1, @message[:payload].scan("test").size)
      end
    end
  end

  # Regression for the lost-update race: #buffer / #buffer_many must mutate the shared @messages
  # buffer under @buffer_mutex. flush/purge/close swap @messages for a fresh array under that lock,
  # so an unguarded append could land in the orphaned old array and be silently dropped. We prove
  # the guard deterministically by holding @buffer_mutex and asserting the mutation cannot proceed
  # until it is released (without the guard it completes immediately despite the held lock).
  describe "buffer mutation locking" do
    it "performs the #buffer append while holding @buffer_mutex" do
      buffer_mutex = @producer.instance_variable_get(:@buffer_mutex)
      done = false

      buffer_mutex.lock

      worker = Thread.new do
        @producer.buffer(build(:valid_message))
        done = true
      end

      # Give the worker ample time to reach (and, with the guard, block on) the append.
      sleep(0.2)
      blocked = !done

      buffer_mutex.unlock
      worker.join(5)

      assert(blocked, "#buffer must block on @buffer_mutex while it is held")
      assert(done)
      refute_empty(@producer.messages)
    end

    it "performs the #buffer_many concat while holding @buffer_mutex" do
      buffer_mutex = @producer.instance_variable_get(:@buffer_mutex)
      done = false

      buffer_mutex.lock

      worker = Thread.new do
        @producer.buffer_many([build(:valid_message), build(:valid_message)])
        done = true
      end

      sleep(0.2)
      blocked = !done

      buffer_mutex.unlock
      worker.join(5)

      assert(blocked, "#buffer_many must block on @buffer_mutex while it is held")
      assert(done)
      assert_equal(2, @producer.messages.size)
    end
  end

  describe "#flush_async" do
    context "when there are no messages in the buffer" do
      it { assert_equal([], @producer.flush_async) }
    end

    context "when there are messages in the buffer" do
      before { @producer.buffer(build(:valid_message)) }

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.flush_async[0]) }
      it { assert_empty(@producer.tap(&:flush_async).messages) }
    end

    context "when an error occurred during flushing" do
      before do
        @error = Rdkafka::RdkafkaError.new(0)
      end

      it do
        @producer.client.stubs(:produce).raises(@error)
        @producer.buffer(build(:valid_message))
        assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.flush_async }
      end
    end
  end

  describe "#flush_sync" do
    context "when there are no messages in the buffer" do
      it { assert_equal([], @producer.flush_sync) }
    end

    context "when there are messages in the buffer" do
      before { @producer.buffer(build(:valid_message)) }

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.flush_sync[0]) }
      it { assert_empty(@producer.tap(&:flush_sync).messages) }
    end

    context "when an error occurred during flushing" do
      before do
        @error = Rdkafka::RdkafkaError.new(0)
      end

      it do
        @producer.client.stubs(:produce).raises(@error)
        @producer.buffer(build(:valid_message))
        assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.flush_sync }
      end
    end
  end

  context "when we have data in the buffer" do
    before { @producer.buffer(build(:valid_message)) }

    it "expect not to allow for a disconnect" do
      refute(@producer.disconnect)
    end
  end

  # Regression guard for #474 resurfacing through the #892 requeue path.
  describe "middleware on the failed-flush retry path" do
    before do
      @middleware = lambda do |message|
        message[:payload] += "-mw"
        message
      end

      @producer.middleware.append(@middleware)
    end

    it "does not re-apply middleware to a message re-buffered after a ProduceManyError" do
      message = build(:valid_message, payload: "value")

      # The first flush fails during dispatch, so the message is re-buffered for a retry
      @producer.client.stubs(:produce).raises(Rdkafka::RdkafkaError.new(0))
      @producer.buffer(message)
      assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.flush_sync }

      # The retry succeeds; middleware must not run a second time on the re-buffered message
      @producer.client.unstub(:produce)
      @producer.flush_sync

      assert_equal("value-mw", message[:payload])
      assert_equal(1, message[:payload].scan("-mw").size)
    end

    it "does not re-apply middleware to messages re-buffered after a MessageInvalidError" do
      valid = build(:valid_message, payload: "value")
      # Valid payload but invalid topic, so validation aborts the batch before anything dispatches
      invalid = build(:valid_message, topic: "bad topic!", payload: "bad")
      @producer.buffer_many([valid, invalid])

      assert_raises(@invalid_error) { @producer.flush_sync }
      assert_equal("value-mw", valid[:payload])

      # The invalid message keeps the retry failing, but middleware must not run again on either
      assert_raises(@invalid_error) { @producer.flush_sync }
      assert_equal("value-mw", valid[:payload])
      assert_equal(1, valid[:payload].scan("-mw").size)
    end
  end

  # #892 restored the buffer for the two error types it enumerated. Anything else raised before a
  # message reaches librdkafka still emptied both buffers and lost the batch outright.
  describe "a pre-dispatch failure" do
    before do
      @boom = ->(_message) { raise "boom from middleware" }
    end

    it "keeps the whole batch buffered when a middleware step raises" do
      @producer.buffer_many(Array.new(5) { build(:valid_message) })
      @producer.middleware.append(@boom)

      assert_raises(RuntimeError) { @producer.flush_async }

      assert_equal(5, @producer.messages.size)
    end

    it "restores the fresh and the already-processed messages to their own buffers" do
      transform = lambda do |message|
        message[:payload] += "-mw"
        message
      end

      @producer.middleware.append(transform)

      # A failed dispatch puts this one in the retry buffer, already middleware-processed
      retried = build(:valid_message, payload: "retried")
      @producer.client.stubs(:produce).raises(Rdkafka::RdkafkaError.new(0))
      @producer.buffer(retried)
      assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.flush_sync }

      # Raise ahead of the transforming step, so the fresh message is untouched when it unwinds
      fresh = build(:valid_message, payload: "fresh")
      @producer.buffer(fresh)
      @producer.middleware.prepend(@boom)

      assert_raises(RuntimeError) { @producer.flush_sync }

      # Fresh messages go back un-transformed so middleware still runs on them exactly once later,
      # while the retried one keeps the single pass it already had
      assert_equal([fresh], @producer.instance_variable_get(:@messages))
      assert_equal("fresh", fresh[:payload])
      assert_equal([retried], @producer.instance_variable_get(:@requeued))
      assert_equal(1, retried[:payload].scan("-mw").size)
    end

    context "when the step raises part-way through the batch" do
      before do
        @armed = true
        @boom_on_m2 = lambda do |message|
          raise "boom from middleware" if @armed && message[:payload].start_with?("m2")

          message
        end

        @messages = Array.new(5) { |i| build(:valid_message, payload: "m#{i}") }
      end

      it "does not transform the already-processed messages again on retry" do
        transform = lambda do |message|
          message[:payload] += "-mw"
          message
        end

        @producer.middleware.append(@boom_on_m2)
        @producer.middleware.append(transform)
        @producer.buffer_many(@messages)

        assert_raises(RuntimeError) { @producer.flush_sync }

        assert_equal(@messages[0..1], @producer.instance_variable_get(:@requeued))
        assert_equal(@messages[2..], @producer.instance_variable_get(:@messages))

        @armed = false
        @producer.flush_sync

        assert_empty(@producer.messages)
        assert_equal(%w[m0-mw m1-mw m2-mw m3-mw m4-mw], @messages.map { |message| message[:payload] })
      end

      it "keeps the transformed copies returned by copy-style middleware" do
        @producer.middleware.append(@boom_on_m2)
        @producer.middleware.append(->(message) { message.merge(payload: "#{message[:payload]}-mw") })
        @producer.buffer_many(@messages)

        assert_raises(RuntimeError) { @producer.flush_sync }

        requeued = @producer.instance_variable_get(:@requeued)

        assert_equal(%w[m0-mw m1-mw], requeued.map { |message| message[:payload] })
        assert_equal(%w[m2 m3 m4], @producer.instance_variable_get(:@messages).map { |m| m[:payload] })
      end

      # Requeueing the failing message would dispatch it without the rest of its chain, so it stays
      # on the middleware path even though the steps before the raise already ran on it
      it "keeps the message whose step raised on the middleware path" do
        transform = lambda do |message|
          message[:payload] += "-mw"
          message
        end

        @producer.middleware.append(transform)
        @producer.middleware.append(->(message) { message[:payload].start_with?("m2") ? raise("boom") : message })
        @producer.buffer_many(@messages)

        assert_raises(RuntimeError) { @producer.flush_sync }

        assert_equal(@messages[0..1], @producer.instance_variable_get(:@requeued))
        assert_same(@messages[2], @producer.instance_variable_get(:@messages).first)
      end

      # An in-place step that ran before the raising step must be rolled back, otherwise the
      # re-buffered message carries a partial transform and the next flush runs the whole chain
      # over it again (double `-mw`).
      it "does not double-apply an in-place step to the message whose later step raised" do
        transform = lambda do |message|
          message[:payload] += "-mw"
          message
        end

        @producer.middleware.append(transform)
        @producer.middleware.append(@boom_on_m2)
        @producer.buffer_many(@messages)

        assert_raises(RuntimeError) { @producer.flush_sync }

        # m2 is re-buffered pristine: the in-place `-mw` from before the raise has been undone
        assert_equal(%w[m2 m3 m4], @producer.instance_variable_get(:@messages).map { |m| m[:payload] })

        @armed = false
        @producer.flush_sync

        assert_empty(@producer.messages)
        assert_equal(%w[m0-mw m1-mw m2-mw m3-mw m4-mw], @messages.map { |message| message[:payload] })
        assert_equal(1, @messages[2][:payload].scan("-mw").size)
      end
    end
  end
end
