module WaterDrop
  # Null object for logger
  # Is used when logger is not defined
  class NullLogger
    # Returns nil for any method call
    def self.method_missing(*_args, &_block)
      nil
    end
  end
end
