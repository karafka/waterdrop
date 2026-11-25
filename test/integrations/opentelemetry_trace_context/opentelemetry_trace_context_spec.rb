# frozen_string_literal: true

# Integration test for the OpenTelemetry vendor middleware installed on all producers.
#
# A producer configured after `#install` injects the W3C `traceparent` of each producer span into
# the Kafka message headers, for single and batch produce. A consumer reads it back from Kafka.
# A `traceparent` set by the user reaches Kafka unchanged.

require "waterdrop"
require "opentelemetry/sdk"
require "waterdrop/instrumentation/vendors/opentelemetry/middleware"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
USER_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

def consume_headers(topic, expected)
  consumer = Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "#{topic}-verifier",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false
  ).consumer

  consumer.subscribe(topic)

  headers = {}
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30

  while headers.size < expected && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    message = consumer.poll(1_000)
    headers[message.payload] = message.headers if message
  end

  headers
ensure
  consumer&.close
end

exporter = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new

OpenTelemetry::SDK.configure do |config|
  config.add_span_processor(OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter))
end

middleware = WaterDrop::Instrumentation::Vendors::OpenTelemetry::Middleware.new do |config|
  config.tracer = OpenTelemetry.tracer_provider.tracer("waterdrop-integration")
end

middleware.install

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

topic = generate_topic("otel-trace-context")
create_topic(topic)

producer.produce_sync(topic: topic, payload: "single")
producer.produce_many_sync(
  [
    { topic: topic, payload: "batch-0" },
    { topic: topic, payload: "batch-1" }
  ]
)
producer.produce_sync(
  topic: topic,
  payload: "user",
  headers: { "traceparent" => USER_TRACEPARENT }
)

producer.close

spans = exporter.finished_spans
expected = spans.map { |span| "00-#{span.hex_trace_id}-#{span.hex_span_id}-01" }
headers = consume_headers(topic, 4)

unless spans.size == 4 && spans.all? { |span| span.name == "send #{topic}" && span.kind == :producer }
  warn "FAIL: expected 4 producer spans, got #{spans.map { |span| [span.name, span.kind] }}"
  exit(1)
end

received = %w[single batch-0 batch-1].map { |payload| headers.dig(payload, "traceparent") }

unless received == expected.first(3)
  warn "FAIL: expected traceparents #{expected.first(3)}, got #{received}"
  exit(1)
end

unless headers.dig("user", "traceparent") == USER_TRACEPARENT
  warn "FAIL: user traceparent was replaced with #{headers.dig("user", "traceparent").inspect}"
  exit(1)
end

puts "PASS: producer spans propagate their trace context through Kafka headers"
