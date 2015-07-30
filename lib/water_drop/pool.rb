module WaterDrop
  # Raw poseidon connection pool for WaterDrop events delivery
  module Pool
    extend SingleForwardable
    # Delegate directly to pool
    def_delegators :pool, :with

    class << self
      # @return [::ConnectionPool] connection pool instance that we can then use
      def pool
        @pool ||= ConnectionPool.new(
          size: ::WaterDrop.config.connection_pool_size,
          timeout: ::WaterDrop.config.connection_pool_timeout
        ) do
          addresses = ::WaterDrop.config.kafka_ports.map do |port|
            "#{::WaterDrop.config.kafka_host}:#{port}"
          end

          Poseidon::Producer.new(addresses, object_id.to_s)
        end
      end
    end
  end
end
