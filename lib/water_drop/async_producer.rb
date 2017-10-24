# frozen_string_literal: true

module WaterDrop
  # Async producer for messages
  AsyncProducer = Class.new(BaseProducer)
  AsyncProducer.method_name = :deliver_async
end
