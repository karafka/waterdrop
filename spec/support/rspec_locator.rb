# frozen_string_literal: true

require 'fileutils'

# RSpec extension for the `RSpec.describe` subject class auto-discovery
# It automatically detects the class name that should be described in the given spec
# based on the spec file path.
#
# @note This locator will be removed once karafka-core one drops active support usage
class RSpecLocator < Module
  # @param  spec_helper_file_path [String] path to the spec_helper.rb file
  # @param inflections [Hash<String, String>] optional inflections map
  def initialize(spec_helper_file_path, inflections = {})
    super()
    @inflections = inflections
    @specs_root_dir = ::File.dirname(spec_helper_file_path)
  end

  # Builds needed API
  # @param rspec [Module] RSpec main module
  def extended(rspec)
    super
    this = self
    # Allows "auto subject" definitions for the `#describe` method, as it will figure
    # out the proper class that we want to describe
    # @param block [Proc] block with specs
    rspec.define_singleton_method :describe_current do |&block|
      describe(this.inherited, &block)
    end
  end

  # @return [Class] class name for the RSpec `#describe` method
  def inherited
    caller(2..2)
      .first
      .split(':')
      .first
      .gsub(@specs_root_dir, '')
      .gsub('_spec.rb', '')
      .split('/')
      .delete_if(&:empty?)
      .itself[1..]
      .join('/')
      .then { |path| custom_camelize(path) }
      .then { |string| transform_inflections(string) }
      .then { |class_name| custom_constantize(class_name) }
  end

  private

  # @param string [String] string we want to cast
  # @return [String] string after inflections
  def transform_inflections(string)
    string = string.dup
    @inflections.each { |from, to| string.gsub!(from, to) }
    string
  end

  # Custom implementation of camelize without ActiveSupport
  # @param string [String] underscored string to convert to CamelCase
  # @return [String] camel-case string
  def custom_camelize(string)
    # First, replace slashes with :: for proper namespacing
    string = string.gsub('/', '::')

    # Then camelize each segment
    string.gsub(/(?:^|_|::)([a-z])/) do |match|
      # If it's a namespace separator, keep it and uppercase the following letter
      if match.include?('::')
        "::#{match[-1].upcase}"
      else
        match[-1].upcase
      end
    end
  end

  # Custom implementation of constantize without ActiveSupport
  # @param string [String] string representing a constant name
  # @return [Class, Module] the constant
  def custom_constantize(string)
    names = string.split('::')
    constant = Object

    names.each do |name|
      # Make sure we're dealing with a valid constant name
      unless name.match?(/^[A-Z][a-zA-Z0-9_]*$/)
        raise NameError, "#{name} is not a valid constant name!"
      end

      # Get the constant from its parent
      constant = constant.const_get(name)
    end

    constant
  rescue NameError => e
    raise NameError, "Uninitialized constant #{string}: #{e.message}"
  end
end
