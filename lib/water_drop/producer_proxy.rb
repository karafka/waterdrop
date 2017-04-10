module WaterDrop
  # Proxy object for a producer (sender) objects that are inside pool
  # We use it to provide additional timeout monitoring layer
  # There used to be an issue with Poseidon (previous engine for this lib)
  # usage of sockets that are old and not used - that's why we just
  # reinitialize connection if the connection layer is not being used for too long
  # We keep this logic to avoid problems just in case. If those problems won't occure
  # with Ruby-Kafka, we will drop it
  class ProducerProxy
    # How long should be object considered alive if nothing is being
    # send using it. After that time, we will recreate the connection
    LIFE_TIME = 5 * 60 #  5 minute

    # If sending fails - how many times we should try with a new connection
    MAX_SEND_RETRIES = 1

    # @return [WaterDrop::ProducerProxy] proxy object to Kafka::Producer
    # @note To ignore @last_usage nil case - we just assume that it is being
    #   first used when we create it
    def initialize
      touch
      @attempts = 0
    end

    # Sends message to Kafka
    # @param message [WaterDrop::Message] message that we want to send
    # @note If something goes wrong it will assume that producer is corrupted and will try to
    #   create a new one
    # @example Send 1 message
    #   ProducerProxy.new.send_message(WaterDrop::Message.new(topic, message))
    def send_message(message)
      touch
      producer.produce(message.message, {
        topic: message.topic
      }.merge(message.options))
      producer.deliver_messages
    rescue StandardError => e
      reload!

      retry if (@attempts += 1) <= MAX_SEND_RETRIES

      raise(e)
    ensure
      @attempts = 0
    end

    private

    # Refreshes last usage value with current time
    def touch
      @last_usage = Time.now
    end

    # @return [Kafka::Producer] producer instance to which we can forward method requests
    def producer
      reload! if dead?
      @producer ||= Kafka.new(seed_brokers: ::WaterDrop.config.kafka.hosts).async_producer
    end

    # @return [Boolean] true if we cannot use producer anymore because it was not used for a
    #   long time
    def dead?
      @last_usage + LIFE_TIME < Time.now
    end

    # Resets a producer so a new one will be created once requested
    def reload!
      @producer.shutdown if @producer
      @producer = nil
    end
  end
end
