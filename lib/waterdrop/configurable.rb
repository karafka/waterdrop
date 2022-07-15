# frozen_string_literal: true

module WaterDrop
  module Configurable
    # A simple settings layer that works similar to dry-configurable
    # It allows us to define settings on a class and per instance level with templating on a class
    # level. It handles inheritance and allows for nested settings.
    #
    # @note The core settings template needs to be defined on a class level
    class << self
      # Sets up all the class methods and inits the core root node.
      # Useful when only per class settings are needed as does not include instance methods
      def extended(base)
        base.extend ClassMethods
      end

      # Sets up all the class and instance methods and inits the core root node
      #
      # @param base [Class] class to which we want to add configuration
      #
      # Needs to be used when per instance configuration is needed
      def included(base)
        base.include InstanceMethods
        base.extend self
      end
    end

    # Instance related methods
    module InstanceMethods
      # @return [Node] config root node
      def config
        @config ||= self.class.config.deep_dup
      end

      # Allows for a per instance configuration (if needed)
      # @param block [Proc] block for configuration
      def configure(&block)
        config.configure(&block)
      end
    end

    # Class related methods
    module ClassMethods
      # @return [Node] root node for the settings
      def config
        return @config if @config

        # This will handle inheritance
        @config = if superclass.respond_to?(:config)
          superclass.config.deep_dup
        else
          Node.new(:root)
        end
      end

      # Allows for a per class configuration (if needed)
      # @param block [Proc] block for configuration
      def configure(&block)
        config.configure(&block)
      end

      # Pipes the settings setup to the config root node
      def setting(...)
        config.setting(...)
      end
    end
  end
end
