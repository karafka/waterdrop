module WaterDrop
  # Event module which encapsulate single Kafka event logic and its delivery
  class Event
    attr_reader :topic, :message

    # @param topic [String, Symbol] a topic to which we want to send a message
    # @param message [Object] any object that can be serialized to a JSON string or
    #   that can be casted to a string
    # @return [WaterDrop::Event] WaterDrop event instance
    # @example Creating a new event
    #   WaterDrop::Event.new(topic, message)
    def initialize(topic, message)
      @topic = topic.to_s
      @message = message.respond_to?(:to_json) ? message.to_json : message.to_s
    end

    # Sents a current event to Kafka
    # @note Won't send any events if send_events config flag is set to false
    # @example Set a message
    #   WaterDrop::Event.new(topic, message).send!
    def send!
      return true unless ::WaterDrop.config.send_events?

      Pool.with do |producer|
        producer.send_messages([
          Poseidon::MessageToSend.new(topic, message)
        ])
      end
    end
  end
end
