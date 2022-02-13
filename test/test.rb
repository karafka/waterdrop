require 'waterdrop'
require 'datadog/statsd'

statsd = Datadog::Statsd.new('localhost', 8125)

require 'waterdrop/instrumentation/vendors/datadog/listener.rb'

producer = WaterDrop::Producer.new

producer.setup do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'request.required.acks': 1,
    'statistics.interval.ms': 1_000
  }
end

listener = ::WaterDrop::Instrumentation::Vendors::Datadog::Listener.new do |config|
  config.client = statsd
end

producer.monitor.subscribe listener

def topic
  res = rand(1..10)
  topics = %w[my-topic other-topic third-topic]

  return topics[0] if res <= 6
  return topics[1] if res <= 9
  return topics[2]
end

10000000.times do
  sleep(0.5)

  rand(3..6).times do
    producer.produce_sync(topic: topic, payload: 'my message')
    producer.produce_async(topic: topic, payload: 'my message')
    producer.produce_async(topic: topic, payload: 'my message')
  end

  rand(5..20).times do
    producer.buffer(topic: topic, payload: 'my message')
  end

  producer.flush_sync

  rand(5..20).times do
    producer.buffer(topic: topic, payload: 'my message')
  end

  producer.buffer_many([topic: topic, payload: 'my message'])

  producer.flush_async
end


#Datadog.configure do |c|
  # Activates and configures an integration
#  c.use :waterdrop, {}
#end
