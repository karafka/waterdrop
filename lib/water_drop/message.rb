module WaterDrop
  # Message class which encapsulate single Kafka message logic and its delivery
  class Message
    attr_reader :topic, :message

    # @param topic [String, Symbol] a topic to which we want to send a message
    # @param message [Object] any object that can be serialized to a JSON string or
    #   that can be casted to a string
    # @return [WaterDrop::Message] WaterDrop message instance
    # @example Creating a new message
    #   WaterDrop::Message.new(topic, message)
    def initialize(topic, message)
      @topic = topic.to_s
      @message = message
    end

    # Sents a current message to Kafka
    # @note Won't send any messages if send_messages config flag is set to false
    # @example Set a message
    #   WaterDrop::Message.new(topic, message).send!
    def send!
      return true unless ::WaterDrop.config.send_messages?

      Pool.with { |producer| producer.send_message(self) }

      ::WaterDrop.logger.info("Message #{message} was sent to topic '#{topic}'")
    rescue StandardError => e
      # Even if we dont reraise this exception, it should log that it happened
      ::WaterDrop.logger.error(e)
      # Reraise if we want to raise on failure
      # Ignore if we dont want to know that something went wrong
      return unless ::WaterDrop.config.raise_on_failure?
      raise(e)
    end
  end
end
