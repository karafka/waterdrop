# frozen_string_literal: true

# WaterDrop library
module WaterDrop
  # Async producer for messages
  AsyncProducer = Class.new(BaseProducer)
  AsyncProducer.method_name = :deliver_async
end
