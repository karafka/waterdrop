# frozen_string_literal: true

module WaterDrop
  class CallbacksManager
    def initialize
      @ar = Concurrent::Hash.new
    end

    def call(*args)
      @ar.each_value { |a| a.call(*args) }
    end

    def add(id, callable)
      @ar[id] = callable
    end

    def delete(id)
      @ar.delete(id)
    end
  end
end
