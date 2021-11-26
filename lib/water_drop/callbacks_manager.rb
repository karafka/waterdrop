# frozen_string_literal: true

module WaterDrop
  # This manager allows us to register multiple callbacks into a hook that is suppose to support
  # a single callback
  class CallbacksManager
    # @return [::WaterDrop::CallbacksManager]
    def initialize
      @callbacks = Concurrent::Hash.new
    end

    # Invokes all the callbacks registered one after another
    #
    # @param args [Object] any args that should go to the callbacks
    def call(*args)
      @callbacks.each_value { |a| a.call(*args) }
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
