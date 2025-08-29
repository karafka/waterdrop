# frozen_string_literal: true

# WaterDrop library
module WaterDrop
  # Connection pool wrapper for WaterDrop producers using the proven connection_pool gem.
  #
  # This provides a clean WaterDrop-specific API while leveraging the battle-tested,
  # connection_pool gem underneath. The wrapper hides the direct usage of the connection_pool
  # gem and provides WaterDrop-specific configuration.
  #
  # @example Basic usage
  #   pool = WaterDrop::ConnectionPool.new(size: 10) do |config|
  #     config.kafka = { 'bootstrap.servers': 'localhost:9092' }
  #     config.deliver = true
  #   end
  #
  #   pool.with do |producer|
  #     producer.produce_sync(topic: 'events', payload: 'data')
  #   end
  #
  # @example Transactional producers with unique IDs
  #   pool = WaterDrop::ConnectionPool.new(size: 5) do |config, index|
  #     config.kafka = {
  #       'bootstrap.servers': 'localhost:9092',
  #       'transactional.id': "my-app-#{index}"
  #     }
  #   end
  #
  # @example Global connection pool
  #   WaterDrop::ConnectionPool.setup(size: 20) do |config|
  #     config.kafka = { 'bootstrap.servers': ENV['KAFKA_BROKERS'] }
  #   end
  #
  #   WaterDrop::ConnectionPool.with do |producer|
  #     producer.produce_async(topic: 'events', payload: 'data')
  #   end
  class ConnectionPool
    # Delegate key methods to underlying connection pool
    extend Forwardable

    def_delegators :@pool, :with, :size, :available

    class << self
      # Global connection pool instance
      attr_accessor :default_pool

      # Sets up a global connection pool
      #
      # @param size [Integer] Pool size (default: 5)
      # @param timeout [Numeric] Connection timeout in seconds (default: 5)
      # @param producer_config [Proc] Block to configure each producer in the pool
      # @yield [config, index] Block to configure each producer in the pool, receives config and
      #   pool index
      # @return [ConnectionPool] The configured global pool
      #
      # @example Basic setup
      #   WaterDrop::ConnectionPool.setup(size: 15) do |config|
      #     config.kafka = { 'bootstrap.servers': ENV['KAFKA_BROKERS'] }
      #     config.deliver = true
      #   end
      #
      # @example Transactional setup with unique IDs
      #   WaterDrop::ConnectionPool.setup(size: 5) do |config, index|
      #     config.kafka = {
      #       'bootstrap.servers': ENV['KAFKA_BROKERS'],
      #       'transactional.id': "my-app-#{index}"
      #     }
      #   end
      def setup(size: 5, timeout: 5, &producer_config)
        ensure_connection_pool_gem!

        @default_pool = new(size: size, timeout: timeout, &producer_config)
      end

      # Executes a block with a producer from the global pool
      #
      # @param block [Proc] Block to execute with a producer
      # @yield [producer] Producer from the global pool
      # @return [Object] Result of the block
      # @raise [RuntimeError] If no global pool is configured
      #
      # @example
      #   WaterDrop::ConnectionPool.with do |producer|
      #     producer.produce_sync(topic: 'events', payload: 'data')
      #   end
      def with(&block)
        raise 'No global connection pool configured. Call setup first.' unless @default_pool

        @default_pool.with(&block)
      end

      # Get statistics about the global pool
      #
      # @return [Hash, nil] Pool statistics or nil if no global pool
      def stats
        return nil unless @default_pool

        {
          size: @default_pool.size,
          available: @default_pool.available
        }
      end

      # Shutdown the global connection pool
      def shutdown
        return unless @default_pool

        @default_pool.shutdown
        @default_pool = nil
      end

      # Reload the global connection pool
      def reload
        @default_pool&.reload
      end

      # Check if the global connection pool is active (configured)
      #
      # @return [Boolean] true if global pool is configured, false otherwise
      def active?
        !@default_pool.nil?
      end

      private

      # Ensures the connection_pool gem is available (class method)
      # Only requires it when actually needed (lazy loading)
      def ensure_connection_pool_gem!
        return if defined?(::ConnectionPool)

        require 'connection_pool'
      rescue LoadError
        raise LoadError, <<~ERROR
          WaterDrop::ConnectionPool requires the 'connection_pool' gem.

          Add this to your Gemfile:
              gem 'connection_pool'

          Then run:
              bundle install
        ERROR
      end
    end

    # Creates a new WaterDrop connection pool
    #
    # @param size [Integer] Pool size (default: 5)
    # @param timeout [Numeric] Connection timeout in seconds (default: 5)
    # @param producer_config [Proc] Block to configure each producer in the pool
    # @yield [config, index] Block to configure each producer in the pool, receives config and
    #   pool index
    def initialize(size: 5, timeout: 5, &producer_config)
      self.class.send(:ensure_connection_pool_gem!)

      @producer_config = producer_config
      @pool_index = 0
      @pool_mutex = Mutex.new

      @pool = ::ConnectionPool.new(size: size, timeout: timeout) do
        producer_index = @pool_mutex.synchronize { @pool_index += 1 }

        WaterDrop::Producer.new do |config|
          if @producer_config.arity == 2
            @producer_config.call(config, producer_index)
          else
            @producer_config.call(config)
          end
        end
      end
    end

    # Get pool statistics
    #
    # @return [Hash] Pool statistics
    def stats
      {
        size: @pool.size,
        available: @pool.available
      }
    end

    # Shutdown the connection pool
    def shutdown
      @pool.shutdown do |producer|
        producer.close! if producer&.status&.active?
      end
    end

    # Reload all connections in the pool
    # Useful for configuration changes or error recovery
    def reload
      @pool.reload do |producer|
        producer.close! if producer&.status&.active?
      end
    end

    # Returns the underlying connection_pool instance
    # This allows access to advanced connection_pool features if needed
    #
    # @return [::ConnectionPool] The underlying connection pool
    attr_reader :pool
  end

  # Convenience methods on the WaterDrop module for global pool access
  class << self
    # Execute a block with a producer from the global connection pool
    # Only available when connection pool is configured
    #
    # @param block [Proc] Block to execute with a producer
    # @yield [producer] Producer from the global pool
    # @return [Object] Result of the block
    #
    # @example
    #   WaterDrop.with do |producer|
    #     producer.produce_sync(topic: 'events', payload: 'data')
    #   end
    def with(&block)
      ConnectionPool.with(&block)
    end

    # Access the global connection pool
    #
    # @return [WaterDrop::ConnectionPool] The global pool
    #
    # @example
    #   WaterDrop.pool.with do |producer|
    #     producer.produce_async(topic: 'events', payload: 'data')
    #   end
    def pool
      ConnectionPool.default_pool
    end
  end
end
