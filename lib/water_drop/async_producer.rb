module WaterDrop
  # Async producer for messages
  AsyncProducer = Class.new(BaseProducer)
  AsyncProducer.method_name = :deliver_async
end
