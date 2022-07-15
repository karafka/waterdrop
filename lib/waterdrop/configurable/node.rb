# frozen_string_literal: true

module WaterDrop
  module Configurable
    # Single non-leaf node
    # This is a core component for the configurable settings
    #
    # The idea here is simple: we collect settings (leafs) and children (nodes) information and we
    # only compile/initialize the values prior to user running the `#configure` API. This API needs
    # to run prior to using the result stuff even if there is nothing to configure
    class Node
      attr_reader :name, :children, :nestings

      attr_accessor :children

      # @param name [Symbol] node name
      # @param block [Proc] block for nested settings
      def initialize(name, nestings = ->(_){})
        @name = name
        @children = []
        @nestings = nestings
        instance_eval(&nestings)
      end

      # Allows for a single leaf or nested node definition
      #
      # @param name [Symbol] setting or nested node name
      # @param default [Object] default value
      # @param constructor [#call, nil] callable or nil
      # @param block [Proc] block for nested settings
      def setting(name, default: nil, constructor: nil, &block)
        @children << if block
          Node.new(name, block)
        else
          Leaf.new(name, default, constructor)
        end
      end

      # Allows for the configuration and setup of the settings
      #
      # Compile settings, allow for overrides via yielding and deep freeze the settings tree
      # @return [Node] returns self after configuration
      def configure
        compile
        yield(self) if block_given?
        deep_freeze
        self
      end

      # @return [Hash] frozen config hash representation
      def to_h
        config = {}

        @children.each do |value|
          config[value.name] = if value.is_a?(Leaf)
                                 public_send(value.name)
                               else
                                 value.to_h
                               end
        end

        config.freeze
      end

      # Freezes all the sub-nodes and the current one
      # It will deep freeze all the children
      # It does not freeze the setting values
      def deep_freeze
        @children.each do |value|
          next unless value.is_a?(Node)

          public_send(value.name).deep_freeze
        end

        freeze
      end

      # Deep copies all the children nodes to allow us for templates buidling on a class level and
      # non-side-effect usage on an instance/inherited.
      # @return [Node] duplicated node
      def deep_dup
        dupped = Node.new(name, nestings)

        dupped.children += children.map do |value|
          value.is_a?(Leaf) ? value.dup : value.deep_dup
        end

        dupped
      end

      # Converts the settings definitions into end children
      # @note It runs once, after things are compiled, they will not be recompiled again
      def compile
        @children.each do |value|
          singleton_class.attr_accessor value.name

          initialized = if value.is_a?(Leaf)
                          value.constructor ? value.constructor.call(value.default) : value.default
                        else
                          value.compile
                          value
                        end

          public_send("#{value.name}=", initialized)
        end
      end
    end
  end
end
