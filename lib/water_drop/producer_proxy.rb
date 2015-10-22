module WaterDrop
  # Proxy object for a producer (sender) objects that are inside pool
  # We use it to provide additional timeout monitoring layer
  # There seem to be some issues with Poseidon and usage of sockets that
  # are old and not used - that's why we just reinitialize connection if
  # the connection layer is not being used for too long
  class ProducerProxy
    # How long should be object considered alive if nothing is being
    # send using it. After that time, we will recreate the connection
    LIFE_TIME = 5 * 60 #  5 minute

    # How often should we refresh meta data from Kafka
    METADATA_REFRESH_INTERVAL = 5 * 60 # 5 minute

    # @see https://kafka.apache.org/08/configuration.html
    # Security level for producer
    REQUIRED_ACKS = -1

    # @return [WaterDrop::ProducerProxy] proxy object to Poseidon::Producer
    # @note To ignore @last_usage nil case - we just assume that it is being
    #   first used when we create it
    def initialize
      touch
    end

    # Sends messages to Kafka
    # @param messages [Array<Poseidon::MessageToSend>] array with messages that we want to send
    # @return [Boolean] were the messages send
    # @note Even if you send one message - it still needs to be in an array
    # @example Send 1 message
    #   ProducerProxy.new.send_messages([Poseidon::MessageToSend.new(topic, message)])
    def send_messages(messages)
      touch
      producer.send_messages(messages)
    end

    private

    # Refreshes last usage value with current time
    def touch
      @last_usage = Time.now
    end

    # @return [Poseidon::Producer] producer instance to which we can forward method requests
    def producer
      reload! if dead?
      # Metadata refresh interval needs to be in miliseconds
      @producer ||= Poseidon::Producer.new(
        ::WaterDrop.config.kafka_hosts,
        producer_id,
        metadata_refresh_interval_ms: METADATA_REFRESH_INTERVAL * 1000,
        required_acks: REQUIRED_ACKS
      )
    end

    # @return [String] random unique id for producer
    def producer_id
      object_id.to_s + Time.now.to_f.to_s
    end

    # @return [Boolean] true if we cannot use producer anymore because it was not used for a
    #   long time
    def dead?
      @last_usage + LIFE_TIME < Time.now
    end

    # Resets a producer so a new one will be created once requested
    def reload!
      @producer = nil
    end
  end
end
