# frozen_string_literal: true

# WaterDrop library
module WaterDrop
  # Sync producer for messages
  SyncProducer = Class.new(BaseProducer)
  SyncProducer.method_name = :deliver
end
