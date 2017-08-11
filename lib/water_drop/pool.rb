# frozen_string_literal: true

module WaterDrop
  # Raw Kafka producers connection pool for WaterDrop messages delivery
  module Pool
    extend SingleForwardable
    # Delegate directly to pool
    def_delegators :pool, :with

    class << self
      # @return [::ConnectionPool] connection pool instance that we can then use
      def pool
        @pool ||= ConnectionPool.new(
          size: ::WaterDrop.config.connection_pool.size,
          timeout: ::WaterDrop.config.connection_pool.timeout
        ) do
          WaterDrop::ProducerProxy.new
        end
      end
    end
  end
end
