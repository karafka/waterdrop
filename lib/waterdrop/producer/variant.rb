# frozen_string_literal: true

module WaterDrop
  class Producer
    # Object that acts as a proxy allowing for alteration of certain low-level per-topic
    # configuration and some other settings that users may find useful to alter, without having
    # to create new producers with their underlying librdkafka instances.
    #
    # Since each librdkafka instance creates at least one TCP connection per broker, creating
    # separate objects just to alter thing like `acks` may not be efficient and may lead to
    # extensive usage of TCP connections, especially in bigger clusters.
    #
    # This variant object allows for "wrapping" of the producer with alteration of those settings
    # in such a way, that two or more alterations can co-exist and share the same producer,
    # effectively sharing the librdkafka client.
    #
    # Since this is an enhanced `SimpleDelegator` all `WaterDrop::Producer` APIs are preserved and
    # a variant alteration can be used as a regular producer. The only important thing is to
    # remember to only close it once.
    #
    # @note Not all settings are alterable. We only allow to alter things that are safe to be
    #   altered as they have no impact on the producer. If there is a setting you consider
    #   important and want to make it alterable, please open a GH issue for evaluation.
    #
    # @note Please be aware, that variant changes also affect buffers. If you overwrite the
    #  `max_wait_timeout`, since buffers are shared (as they exist on producer level), flushing
    #   may be impacted.
    #
    # @note `topic_config` is validated when created for the first time during message production.
    #   This means, that configuration error may be raised only during dispatch. There is no
    #   way out of this, since we need `librdkafka` instance to create the references.
    class Variant < SimpleDelegator
      # Empty hash we use as defaults for topic config.
      # When rdkafka-ruby detects empty hash, it will use the librdkafka defaults
      EMPTY_HASH = {}.freeze

      private_constant :EMPTY_HASH

      attr_reader :max_wait_timeout, :topic_config, :producer

      # @param producer [WaterDrop::Producer] producer for which we want to have a variant
      # @param max_wait_timeout [Integer, nil] alteration to max wait timeout or nil to use
      #   default
      # @param topic_config [Hash] extra topic configuration that can be altered.
      # @param default [Boolean] is this a default variant or an altered one
      # @see https://karafka.io/docs/Librdkafka-Configuration/#topic-configuration-properties
      def initialize(
        producer,
        max_wait_timeout: producer.config.max_wait_timeout,
        topic_config: EMPTY_HASH,
        default: false
      )
        @producer = producer
        @max_wait_timeout = max_wait_timeout
        @topic_config = topic_config
        @default = default
        super(producer)

        Contracts::Variant.new.validate!(to_h, Errors::VariantInvalidError)
      end

      # @return [Boolean] is this a default variant for this producer
      def default?
        @default
      end

      # We need to wrap any methods from our API that could use a variant alteration with the
      # per thread variant injection. Since method_missing can be slow and problematic, it is just
      # easier to use our public API components methods to ensure the variant is being injected.
      [
        Async,
        Buffer,
        Sync,
        Transactions
      ].each do |scope|
        scope.instance_methods(false).each do |method_name|
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{method_name}(*args, &block)
              ref = Fiber.current.waterdrop_clients ||= {}
              ref[@producer.id] = self

              @producer.#{method_name}(*args, &block)
            ensure
              ref[@producer.id] = nil
            end
          RUBY
        end
      end

      private

      # @return [Hash] hash representation for contract validation to ensure basic sanity of the
      #   settings.
      def to_h
        {
          default: default?,
          max_wait_timeout: max_wait_timeout,
          topic_config: topic_config,
          # We pass this to validation, to make sure no-one alters the `acks` value when operating
          # in the transactional mode as it causes librdkafka to crash ruby
          # @see https://github.com/confluentinc/librdkafka/issues/4710
          transactional: @producer.transactional?,
          # We pass this for a similar reason as above
          idempotent: @producer.idempotent?
        }
      end
    end
  end
end
