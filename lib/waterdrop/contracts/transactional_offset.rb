# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract to ensure that arguments provided to the transactional offset commit are valid
    # and match our expectations
    class TransactionalOffset < ::Karafka::Core::Contractable::Contract
      configure do |config|
        config.error_messages = YAML.safe_load(
          File.read(
            File.join(WaterDrop.gem_root, 'config', 'locales', 'errors.yml')
          )
        ).fetch('en').fetch('validations').fetch('transactional_offset')
      end

      required(:consumer) { |val| val.respond_to?(:consumer_group_metadata_pointer) }
      required(:message) { |val| val.respond_to?(:topic) && val.respond_to?(:partition) }
      required(:offset_metadata) { |val| val.is_a?(String) || val.nil? }
    end
  end
end
