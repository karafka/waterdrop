# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'water_drop/version'

Gem::Specification.new do |spec|
  spec.name          = 'waterdrop'
  spec.version       = ::WaterDrop::VERSION
  spec.platform      = Gem::Platform::RUBY
  spec.authors       = ['Maciej Mensfeld']
  spec.email         = %w[maciej@mensfeld.pl]
  spec.homepage      = 'https://github.com/karafka/waterdrop'
  spec.summary       = 'Kafka messaging made easy!'
  spec.description   = spec.summary
  spec.license       = 'MIT'

  spec.add_dependency 'delivery_boy', '~> 0.2'
  spec.add_dependency 'dry-configurable', '~> 0.8'
  spec.add_dependency 'dry-monitor', '~> 0.3'
  spec.add_dependency 'dry-validation', '~> 1.2'
  spec.add_dependency 'ruby-kafka', '>= 0.7.8'
  spec.add_dependency 'zeitwerk', '~> 2.1'

  spec.required_ruby_version = '>= 2.4.0'

  spec.cert_chain  = ['certs/mensfeld.pem']
  spec.signing_key = File.expand_path('~/.ssh/gem-private_key.pem') if $0 =~ /gem\z/

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(spec)/}) }
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = %w[lib]
end
