# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # This manager allows us to register multiple callbacks into a hook that is suppose to support
    # a single callback
    class CallbacksManager
      # @return [::WaterDrop::Instrumentation::CallbacksManager]
      def initialize
        @callbacks = Concurrent::Hash.new
      end

      # Invokes all the callbacks registered one after another
      #
      # @param args [Object] any args that should go to the callbacks
      # @note We do not use `#each_value` here on purpose. With it being used, we cannot dispatch
      #   callbacks and add new at the same time. Since we don't know when and in what thread
      #   things are going to be added to the manager, we need to extract values into an array and
      #   run it. That way we can add new things the same time.
      def call(*args)
        @callbacks.values.each { |callback| callback.call(*args) }
      end

      # Adds a callback to the manager
      #
      # @param id [String] id of the callback (used when deleting it)
      # @param callable [#call] object that responds to a `#call` method
      def add(id, callable)
        @callbacks[id] = callable
      end

      # Removes the callback from the manager
      # @param id [String] id of the callback we want to remove
      def delete(id)
        @callbacks.delete(id)
      end
    end
  end
end
