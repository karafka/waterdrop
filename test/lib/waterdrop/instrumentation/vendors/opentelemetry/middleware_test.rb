# frozen_string_literal: true

require "opentelemetry/sdk"

describe_current do
  before do
    @exporter = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
    @tracer_provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    @tracer_provider.add_span_processor(
      OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(@exporter)
    )
    @tracer = @tracer_provider.tracer("waterdrop-test")
    @propagator = OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator

    @middleware = described_class.new do |config|
      config.tracer = @tracer
      config.propagator = @propagator
    end

    @topic = generate_topic
  end

  describe "when producing a single message" do
    before do
      @message = { topic: @topic, payload: "test", key: "key", partition: 0 }
      @result = @middleware.call(@message)
      @span = @exporter.finished_spans.first
    end

    it "expect to create one finished producer span" do
      assert_equal 1, @exporter.finished_spans.size
      assert_equal "send #{@topic}", @span.name
      assert_equal :producer, @span.kind
    end

    it "expect to set the semconv attributes" do
      assert_equal(
        {
          "messaging.system.name" => "kafka",
          "messaging.operation.name" => "send",
          "messaging.operation.type" => "send",
          "messaging.destination.name" => @topic,
          "messaging.destination.partition.id" => "0",
          "kafka.message.key" => "key"
        },
        @span.attributes.to_h
      )
    end

    it "expect to inject the span context as traceparent" do
      traceparent = "00-#{@span.hex_trace_id}-#{@span.hex_span_id}-01"

      assert_equal traceparent, @result[:headers]["traceparent"]
    end

    it "expect not to mutate the caller message" do
      refute_same @message, @result
      refute @message.key?(:headers)
    end
  end

  describe "when message has no key, no partition and a symbol topic" do
    before do
      @middleware.call(topic: @topic.to_sym, payload: "test", partition: -1)
      @span = @exporter.finished_spans.first
    end

    it "expect to use the topic name and skip the absent attributes" do
      assert_equal "send #{@topic}", @span.name
      assert_equal @topic, @span.attributes["messaging.destination.name"]
      refute @span.attributes.key?("messaging.destination.partition.id")
      refute @span.attributes.key?("kafka.message.key")
    end
  end

  describe "when message has a binary key" do
    before do
      @middleware.call(topic: @topic, payload: "test", key: "\xFF\xFE".b)
      @span = @exporter.finished_spans.first
    end

    it "expect not to set the key attribute" do
      refute @span.attributes.key?("kafka.message.key")
    end
  end

  describe "when message has an ASCII binary key" do
    before do
      @middleware.call(topic: @topic, payload: "test", key: "key".b)
      @span = @exporter.finished_spans.first
    end

    it "expect to set the key attribute" do
      assert_equal "key", @span.attributes["kafka.message.key"]
    end
  end

  describe "when message has a non-ASCII UTF-8 key" do
    before do
      @middleware.call(topic: @topic, payload: "test", key: "klucz-ą")
      @span = @exporter.finished_spans.first
    end

    it "expect to set the key attribute" do
      assert_equal "klucz-ą", @span.attributes["kafka.message.key"]
    end
  end

  describe "when message has an invalid UTF-8 key" do
    before do
      @middleware.call(topic: @topic, payload: "test", key: (+"\xFF").force_encoding(Encoding::UTF_8))
      @span = @exporter.finished_spans.first
    end

    it "expect not to set the key attribute" do
      refute @span.attributes.key?("kafka.message.key")
    end
  end

  describe "when message has user headers" do
    before do
      @headers = { "user" => "value" }
      @message = { topic: @topic, payload: "test", headers: @headers }
      @result = @middleware.call(@message)
    end

    it "expect to keep user headers and add traceparent" do
      assert_equal "value", @result[:headers]["user"]
      assert @result[:headers].key?("traceparent")
    end

    it "expect not to mutate the caller headers" do
      assert_equal({ "user" => "value" }, @headers)
      assert_same @headers, @message[:headers]
      refute_same @headers, @result[:headers]
    end
  end

  describe "when the propagator injects a header the user also set" do
    before do
      propagator = Object.new

      def propagator.inject(carrier, context:)
        carrier["shared"] = "injected"
      end

      @middleware = described_class.new do |config|
        config.tracer = @tracer
        config.propagator = propagator
      end

      @headers = { "shared" => "user", "user" => "value" }.freeze
      @message = { topic: @topic, payload: "test", headers: @headers }
      @result = @middleware.call(@message)
    end

    it "expect the injected value to win and keep the other user headers" do
      assert_equal({ "shared" => "injected", "user" => "value" }, @result[:headers])
    end

    it "expect not to mutate the frozen caller headers" do
      assert_equal({ "shared" => "user", "user" => "value" }, @headers)
      assert_same @headers, @message[:headers]
    end
  end

  describe "when message already has a user traceparent" do
    before do
      @traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
      @message = { topic: @topic, payload: "test", headers: { "traceparent" => @traceparent } }
      @result = @middleware.call(@message)
    end

    it "expect to keep the user traceparent and the message" do
      assert_same @message, @result
      assert_equal @traceparent, @result[:headers]["traceparent"]
    end

    it "expect to still create the producer span" do
      assert_equal 1, @exporter.finished_spans.size
    end
  end

  describe "when spans are off" do
    before do
      @middleware = described_class.new do |config|
        config.tracer = @tracer
        config.propagator = @propagator
        config.spans = false
      end
    end

    it "expect to inject the current context without creating a span" do
      parent = @tracer.start_span("parent")

      result = OpenTelemetry::Context.with_current(
        OpenTelemetry::Trace.context_with_span(parent)
      ) do
        @middleware.call(topic: @topic, payload: "test")
      end

      parent.finish

      assert_equal(
        "00-#{parent.context.hex_trace_id}-#{parent.context.hex_span_id}-01",
        result[:headers]["traceparent"]
      )
      assert_equal %w[parent], @exporter.finished_spans.map(&:name)
    end

    it "expect to return the same message when there is no active span" do
      message = { topic: @topic, payload: "test" }
      result = @middleware.call(message)

      assert_same message, result
      refute result.key?(:headers)
    end

    it "expect to return the same message with headers when there is no active span" do
      message = { topic: @topic, payload: "test", headers: { "user" => "value" } }

      assert_same message, @middleware.call(message)
    end

    it "expect to return the same message when it has a user traceparent" do
      message = { topic: @topic, payload: "test", headers: { "traceparent" => "x" } }

      assert_same message, @middleware.call(message)
    end
  end

  describe "when headers are not a hash" do
    before do
      @message = { topic: @topic, payload: "test", headers: "invalid" }
      @result = @middleware.call(@message)
    end

    it "expect to leave the message for the contract to reject" do
      assert_same @message, @result
      assert_empty @exporter.finished_spans
    end
  end

  describe "when no propagator is set" do
    before do
      @middleware = described_class.new do |config|
        config.tracer = @tracer
      end

      @previous = OpenTelemetry.propagation
      OpenTelemetry.propagation = @propagator
    end

    after { OpenTelemetry.propagation = @previous }

    it "expect to use the global propagator" do
      result = @middleware.call(topic: @topic, payload: "test")

      assert result[:headers].key?("traceparent")
    end
  end

  describe "when no tracer is set" do
    before do
      @middleware = described_class.new do |config|
        config.propagator = @propagator
      end

      @previous = OpenTelemetry.tracer_provider
      OpenTelemetry.tracer_provider = @tracer_provider
    end

    after { OpenTelemetry.tracer_provider = @previous }

    it "expect to use the waterdrop tracer of the global tracer provider" do
      result = @middleware.call(topic: @topic, payload: "test")
      span = @exporter.finished_spans.first

      assert_equal "send #{@topic}", span.name
      assert_equal "waterdrop", span.instrumentation_scope.name
      assert_equal WaterDrop::VERSION, span.instrumentation_scope.version
      assert_equal "00-#{span.hex_trace_id}-#{span.hex_span_id}-01", result[:headers]["traceparent"]
    end
  end

  describe "when used as producer middleware" do
    before do
      @producer = build(:producer)
      @producer.middleware.append(@middleware)
      @message = { topic: @topic, payload: "test" }
    end

    it "expect to inject headers on single produce without mutating the message" do
      report = @producer.produce_sync(@message)

      refute_nil report
      refute @message.key?(:headers)
      assert_equal 1, @exporter.finished_spans.size
    end

    it "expect to create one span and inject headers for each batch message" do
      messages = Array.new(3) { |i| { topic: @topic, payload: i.to_s } }
      injected = []

      @producer.monitor.subscribe("messages.produced_sync") do |event|
        injected.concat(event[:messages].map { |message| message[:headers]["traceparent"] })
      end

      @producer.produce_many_sync(messages)

      spans = @exporter.finished_spans

      assert_equal 3, spans.size
      assert_equal(
        spans.map { |span| "00-#{span.hex_trace_id}-#{span.hex_span_id}-01" },
        injected
      )
      assert(messages.none? { |message| message.key?(:headers) })
    end
  end

  describe "when messages are buffered and flushed in another trace" do
    before do
      @middleware = described_class.new do |config|
        config.tracer = @tracer
        config.propagator = @propagator
        config.spans = false
      end

      @producer = build(:producer)
      @producer.middleware.append(@middleware)
      @dispatched = []
      @producer.middleware.append(->(message) { message.tap { @dispatched << message } })
    end

    it "expect to inject the trace context of the flush caller" do
      buffer_span = @tracer.start_span("buffer")
      flush_span = @tracer.start_span("flush")

      OpenTelemetry::Context.with_current(OpenTelemetry::Trace.context_with_span(buffer_span)) do
        @producer.buffer(topic: @topic, payload: "test")
      end

      OpenTelemetry::Context.with_current(OpenTelemetry::Trace.context_with_span(flush_span)) do
        @producer.flush_sync
      end

      assert_equal(
        ["00-#{flush_span.context.hex_trace_id}-#{flush_span.context.hex_span_id}-01"],
        @dispatched.map { |message| message[:headers]["traceparent"] }
      )
    end
  end

  describe "#install" do
    after { WaterDrop.monitor.unsubscribe(@middleware) }

    it "expect to append the middleware to producers configured afterwards" do
      @middleware.install
      producer = build(:producer)

      result = producer.middleware.run(topic: @topic, payload: "test")

      assert result[:headers].key?("traceparent")
    end

    it "expect not to append the middleware to producers configured before" do
      producer = build(:producer)
      @middleware.install
      message = { topic: @topic, payload: "test" }

      assert_same message, producer.middleware.run(message)
      assert_empty @exporter.finished_spans
    end

    it "expect to append the middleware once when installed twice" do
      @middleware.install
      @middleware.install
      producer = build(:producer)

      producer.middleware.run(topic: @topic, payload: "test")

      assert_equal 1, @exporter.finished_spans.size
    end

    it "expect not to append the middleware after unsubscribing" do
      @middleware.install
      WaterDrop.monitor.unsubscribe(@middleware)
      producer = build(:producer)
      message = { topic: @topic, payload: "test" }

      assert_same message, producer.middleware.run(message)
      assert_empty @exporter.finished_spans
    end
  end
end
