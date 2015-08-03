module WaterDrop
  module Aspects
    # Base class for all aspects
    class BaseAspect < ::Aspector::Base
      default private_methods: true

      # @param this is an instance on which we execute aspect (original method caller)
      # @param [Hash] aspect options
      # @param [Array] originam method arguments
      # @param [Block] block which we evaluate to get a message that we will send
      # @param original method result
      def handle(this, options, args, message, *result)
        formatter = Formatter.new(
          options,
          args,
          instance_run(this, result, message)
        )

        Event.new(options[:topic], formatter.message).send!
      end

      private

      # Method used to change message block binding, so it will be evaluated
      # in the caller instance context
      # @param this is an instance on which we execute aspect (original method caller)
      # @param original method call result
      # @param [Block] message block
      def instance_run(this, result, message)
        return this.instance_eval(&message) if message.parameters.empty?

        this.instance_exec(result, message) { |res, block| block.call(res.first) }
      end
    end
  end
end
