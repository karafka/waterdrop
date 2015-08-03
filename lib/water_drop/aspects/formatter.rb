module WaterDrop
  module Aspects
    # Class used to format message that will be send from an aspect
    class Formatter
      # @param [Hash] options from an aspect
<<<<<<< HEAD
      # @param [Array] original method arguments
=======
      # @param [Array] args original method arguments
>>>>>>> Add documentation
      # @param result of execution of the method
      def initialize(options, args, result)
        @options = options
        @args = args
        @result = result
      end

      # @return [Hash] hash with formatted message that can be send
      def message
        {
          topic:  @options[:topic],
          method: @options[:method],
          message: @result,
          args: @args
        }
      end
    end
  end
end
