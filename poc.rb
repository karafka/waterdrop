require 'waterdrop'

producer = WaterDrop::Producer.new

producer.setup do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers' => 'localhost:9092',
    'request.required.acks' => [-1, 1].sample
  }
end

msg = {
  topic:   "e2r12r1",
  payload: "Payload" * 1,
  key:     "%^&*(",
  partition: -1
}

producer.produce_sync(msg)
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
p producer.flush_async

producer.close
producer.close
