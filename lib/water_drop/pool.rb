module WaterDrop
  # Raw poseidon connection pool for WaterDrop messages delivery
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
          Poseidon::Producer.new(
            ::WaterDrop.config.kafka_hosts,
            object_id.to_s + rand.to_s + Time.now.to_f.to_s
          )
        end
      end
    end
  end
end
