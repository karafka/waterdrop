# frozen_string_literal: true

module WaterDrop
  # Extra internal helper objects
  module Helpers
    # Atomic counter that we can safely increment and decrement without race conditions
    class Counter
      # @return [Integer] current value
      attr_reader :value

      def initialize
        @value = 0
        @mutex = Mutex.new
      end

      # Increments the value by 1
      def increment
        @mutex.synchronize { @value += 1 }
      end

      # Decrements the value by 1
      def decrement
        @mutex.synchronize { @value -= 1 }
      end
    end
  end
end
