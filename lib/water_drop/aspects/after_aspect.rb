module WaterDrop
  module Aspects
    # After method execution aspect
    # @example Apply after aspect to a method
    #   WaterDrop::Aspects::AfterAspect.apply(
    #     ClassName,
    #     method: :run,
    #     topic: 'karafka_topic',
    #     message: ->(result) { "This is result of method run: #{result}" }
    #   )
    class AfterAspect < BaseAspect
      after options[:method], interception_arg: true do |interception, result, *args|
        options = interception.options
        interception.aspect.handle(self, options, args, options[:message], result)
        ::WaterDrop.logger.debug("AfterAspect message was applied after #{self.class.name} class")
        result
      end
    end
  end
end
