# frozen_string_literal: true

module WaterDrop
  # Simple middleware layer for manipulating messages prior to their validation
  class Middleware
    def initialize
      @mutex = Mutex.new
      @steps = []
    end

    # Runs middleware on a single message prior to validation
    #
    # @param message [Hash] message hash
    # @return [Hash] message hash. Either the same if transformed in place, or a copy if modified
    #   into a new object.
    # @note You need to decide yourself whether you don't use the message hash data anywhere else
    #   and you want to save on memory by modifying it in place or do you want to do a deep copy
    def run(message)
      @steps.each do |step|
        message = step.call(message)
      end

      message
    end

    # @param messages [Array<Hash>] messages on which we want to run middlewares
    # @return [Array<Hash>] transformed messages
    def run_many(messages)
      messages.map do |message|
        run(message)
      end
    end

    # Register given middleware as the first one in the chain
    # @param step [#call] step that needs to return the message
    def prepend(step)
      @mutex.synchronize do
        @steps.prepend step
      end
    end

    # Register given middleware as the last one in the chain
    # @param step [#call] step that needs to return the message
    def append(step)
      @mutex.synchronize do
        @steps.append step
      end
    end
  end
end
