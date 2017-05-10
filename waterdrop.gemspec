# frozen_string_literal: true

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'water_drop/version'

Gem::Specification.new do |spec|
  spec.name          = 'waterdrop'
  spec.version       = ::WaterDrop::VERSION
  spec.platform      = Gem::Platform::RUBY
  spec.authors       = ['Maciej Mensfeld', 'Pavlo Vavruk']
  spec.email         = %w[maciej@mensfeld.pl pavlo.vavruk@gmail.com]
  spec.homepage      = 'https://github.com/karafka/waterdrop'
  spec.summary       = ' Kafka messaging made easy! '
  spec.description   = spec.summary
  spec.license       = 'MIT'

  spec.add_dependency 'bundler', '>= 0'
  spec.add_dependency 'rake', '>= 0'
  spec.add_dependency 'ruby-kafka', '>= 0'
  spec.add_dependency 'connection_pool', '>= 0'
  spec.add_dependency 'null-logger'
  spec.add_dependency 'dry-configurable', '~> 0.6'
  spec.required_ruby_version = '>= 2.2.0'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(spec)/}) }
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = %w[lib]
end
