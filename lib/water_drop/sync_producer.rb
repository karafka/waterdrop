module WaterDrop
  # Sync producer for messages
  Producer = SyncProducer = Class.new(BaseProducer)
  SyncProducer.method_name = :deliver
end
