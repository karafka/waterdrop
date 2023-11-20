# frozen_string_literal: true

require 'spec_helper'

RSpec.describe_current do
  subject(:counter) { described_class.new }

  describe '#initialize' do
    it 'starts with a value of 0' do
      expect(counter.value).to eq(0)
    end
  end

  describe '#increment' do
    it 'increases the value by 1' do
      counter.increment
      expect(counter.value).to eq(1)
    end

    it 'correctly increments the value concurrently' do
      threads = Array.new(10) do
        Thread.new { counter.increment }
      end
      threads.each(&:join)
      expect(counter.value).to eq(10)
    end
  end

  describe '#decrement' do
    it 'decreases the value by 1' do
      counter.decrement
      expect(counter.value).to eq(-1)
    end

    it 'correctly decrements the value concurrently' do
      threads = Array.new(10) do
        Thread.new { counter.decrement }
      end
      threads.each(&:join)
      expect(counter.value).to eq(-10)
    end
  end

  describe 'concurrent increment and decrement' do
    it 'correctly updates the value with concurrent increments and decrements' do
      increment_threads = Array.new(5) { Thread.new { counter.increment } }
      decrement_threads = Array.new(5) { Thread.new { counter.decrement } }
      (increment_threads + decrement_threads).each(&:join)
      expect(counter.value).to eq(0)
    end
  end
end
