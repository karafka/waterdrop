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
    # @note Applying the chain is atomic: a step raising part-way through restores the message to
    #   its pre-run state before re-raising. This keeps middleware applied exactly once even for
    #   in-place steps, since a message re-buffered after a failed flush is not left half
    #   transformed for the next flush to run the whole chain over again.
    def run(message)
      return message if @count.zero?

      # Snapshot before running so an in-place step that already ran can be rolled back if a later
      # step raises. `message` may be reassigned by copy-style steps, so roll back the original.
      original = message
      snapshot = message.dup

      @steps.each do |step|
        message = step.call(message)
      end

      message
    rescue
      original.replace(snapshot)
      raise
    end

    # @param messages [Array<Hash>] messages on which we want to run middlewares
    # @return [Array<Hash>] transformed messages or same messages if no transformation
    # @note Each message is run atomically (see {#run}): a step raising restores the failing
    #   message before re-raising, so it is never left partially transformed.
    def run_many(messages)
      # Skip middleware processing entirely if no middleware steps are configured
      return messages if @count.zero?

      # Use each_with_object to avoid creating intermediate arrays for large batches
      messages.each_with_object([]) do |message, result|
        result << run(message)
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
