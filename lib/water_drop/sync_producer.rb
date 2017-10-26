# frozen_string_literal: true

# WaterDrop library
module WaterDrop
  # Sync producer for messages
  SyncProducer = Class.new(BaseProducer)
  # Sync producer for messages
  Producer = SyncProducer
  SyncProducer.method_name = :deliver
end
