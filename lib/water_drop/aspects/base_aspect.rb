module WaterDrop
  # Aspects module which include all available aspects
  module Aspects
    # Base class for all aspects
    class BaseAspect < ::Aspector::Base
      default private_methods: true

      # @param this is an instance on which we execute aspect (original method caller)
      # @param [Hash] options aspect options
      # @param [Array] args original method arguments
      # @param [Block] message block which we evaluate to get a message that we will send
      # @param result original method result
      def handle(this, options, args, message, *result)
        formatter = Formatter.new(
          options,
          args,
          instance_run(this, result, message)
        )

        Message.new(options[:topic], formatter.message).send!
      end

      # @param this is an instance on which we execute aspect (original method caller)
      # @param [Hash] options aspect options
      # @param [String, Symbol] position where aspect was applied (before, after)
      # @example
      #   If we apply aspect to Calculator.sum method in different ways(AroundAspect, BeforeAspect)
      #
      #   interception.aspect.log(self, options) will print
      #   WaterDrop::Aspects::BeforeAspect message was applied to Calculator.sum method
      #
      #   interception.aspect.log(self, options, :before) will print
      #   WaterDrop::Aspects::AroundAspect before_message was applied to Calculator.sum method
      def log(this, options, *position)
        method = "#{this.class.name}.#{options[:method]}"
        prefix = "#{position.first}_" unless position.empty?
        aspect = self.class.name

        ::WaterDrop.logger.debug("#{aspect} #{prefix}message was applied to #{method} method")
      end

      private

      # Method used to change message block binding, so it will be evaluated
      # in the caller instance context
      # @param this is an instance on which we execute aspect (original method caller)
      # @param result original method call result
      # @param [Block] message block
      def instance_run(this, result, message)
        return this.instance_eval(&message) if message.parameters.empty?

        this.instance_exec(result, message) { |res, block| block.call(res.first) }
      end
    end
  end
end
