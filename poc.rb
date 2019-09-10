# frozen_string_literal: true

require 'waterdrop'

producer = WaterDrop::Producer.new

producer.setup do |config|
  config.logger = Logger.new($stdout, level: Logger::INFO)
  config.kafka = {
    'bootstrap.servers' => 'localhost:9092',
    'request.required.acks' => 1
  }
end

producer.monitor.subscribe(
  WaterDrop::Instrumentation::StdoutListener.new(
    producer.config.logger
  )
)

msg = {
  topic: 'e2r12r1',
  payload: 'X' * 2048*1000,
  key: '%^&*(',
  partition: -1
}

100.times do
  producer.produce_sync(msg)
end

producer.produce_async(msg)
producer.produce_many_sync(Array.new(10) { msg })
producer.produce_many_async(Array.new(10) { msg })

producer.buffer(msg)
producer.flush_sync

producer.buffer(msg)
producer.flush_async

producer.buffer_many(Array.new(10) { msg })
producer.flush_sync

producer.buffer_many(Array.new(10) { msg })
producer.flush_async

producer.buffer_many(Array.new(10) { msg })
# producer.close


handlers = producer.produce_many_async(Array.new(100) { msg })

handlers.map(&:wait)
nil
