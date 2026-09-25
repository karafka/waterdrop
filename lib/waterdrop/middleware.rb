# frozen_string_literal: true

module WaterDrop
  # Simple middleware layer for manipulating messages prior to their validation
  class Middleware
    # Creates a new middleware chain with no registered steps
    def initialize
      @mutex = Mutex.new
      @steps = []
      @count = 0
    end

    # Runs middleware on a single message prior to validation
    #
    # @param message [Hash] message hash
    # @return [Hash] message hash. Either the same if transformed in place, or a copy if modified
    #   into a new object.
    # @note You need to decide yourself whether you don't use the message hash data anywhere else
    #   and you want to save on memory by modifying it in place or do you want to do a deep copy
    # @note Steps run sequentially and a chain run is not atomic. If an in-place step mutates the
    #   message and a later step raises, the message is left partially transformed. During a
    #   buffer flush that message is re-buffered as not yet processed, so the next flush runs the
    #   whole chain over it again and the earlier in-place steps are applied twice (for example
    #   double encryption or duplicated headers). Making the run atomic would mean copying every
    #   message before its chain runs, which defeats the memory saving in-place mode is for, so
    #   this is a deliberate trade-off. If an in-place step can be followed by a step that may
    #   raise, make the in-place step idempotent or have it return a copy instead.
    def run(message)
      return message if @count.zero?

      @steps.each do |step|
        message = step.call(message)
      end

      message
    end

    # @param messages [Array<Hash>] messages on which we want to run middlewares
    # @return [Array<Hash>] transformed messages or same messages if no transformation
    def run_many(messages)
      # Skip middleware processing entirely if no middleware steps are configured
      return messages if @count.zero?

      # Use each_with_object to avoid creating intermediate arrays for large batches
      messages.each_with_object([]) do |message, result|
        @steps.each do |step|
          message = step.call(message)
        end

        result << message
      end
    end

    # Register given middleware as the first one in the chain
    # @param step [#call] step that needs to return the message
    def prepend(step)
      @mutex.synchronize do
        @steps.prepend step
        @count = @steps.size
      end
    end

    # Register given middleware as the last one in the chain
    # @param step [#call] step that needs to return the message
    def append(step)
      @mutex.synchronize do
        @steps.append step
        @count = @steps.size
      end
    end
  end
end
