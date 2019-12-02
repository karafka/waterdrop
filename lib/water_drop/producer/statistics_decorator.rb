# frozen_string_literal: true

module WaterDrop
  class Producer
    # Many of the librdkafka statistics are absolute values instead of a gauge.
    # This means, that for example number of messages sent is an absolute growing value
    # instead of being a value of messages sent from the last statistics report.
    # This decorator calculates the diff against previously emited stats, so we get also
    # the diff together with the original values
    class StatisticsDecorator
      def initialize
        @previous = {}.freeze
      end

      # @param emited_stats [Hash] original emited statistics
      # @return [Hash] emited statistics extended with the diff data
      # @note We modify the emited statistics, instead of creating new. Since we don't expose
      #   any API to get raw data, users can just assume that the result of this decoration is the
      #   proper raw stats that they can use
      def call(emited_stats)
        diff(
          @previous,
          emited_stats
        )

        @previous = emited_stats

        emited_stats.freeze
      end

      private

      # Calculates the diff of the provided values and modifies in place the emited statistics
      #
      # @param previous [Object] previous value from the given scope in which
      #   we are
      # @param current [Object] current scope from emitted statistics
      # @return [Object] the diff if the values were numerics or the current scope
      def diff(previous, current)
        if current.is_a?(Hash)
          # @note We cannot use #each_key as we modify the content of the current scope
          #   in place (in case it's a hash)
          current.keys.each do |key|
            append(
              current,
              key,
              diff((previous || {})[key], (current || {})[key])
            )
          end
        end

        if current.is_a?(Numeric) && previous.is_a?(Numeric)
          current - previous
        else
          current
        end
      end

      # Appends the result of the diff to a given key as long as the result is numeric
      #
      # @param current [Hash] current scope
      # @param key [Symbol] key based on which we were diffing
      # @param result [Object] diff result
      def append(current, key, result)
        return unless result.is_a?(Numeric)

        current["#{key}_d".to_sym] = result
      end
    end
  end
end
