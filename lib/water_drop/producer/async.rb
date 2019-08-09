# frozen_string_literal: true

module WaterDrop
  class Producer
    module Async
      def produce_async(message)
        ensure_active!
        @validator.call(message)

        @monitor.instrument(
          'message.produced_async',
          producer: self,
          message: message
        ) { @client.produce(message) }
      end

      def produce_many_async(messages)
        ensure_active!
        messages.each(&@validator)

        @monitor.instrument(
          'messages.produced_async',
          producer: self,
          messages: messages
        ) do
          messages.map { |message| @client.produce(message) }
        end
      end
    end
  end
end
