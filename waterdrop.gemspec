lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'rake'
require 'water_drop/version'

Gem::Specification.new do |spec|
  spec.name          = 'waterdrop'
  spec.version       = ::WaterDrop::VERSION
  spec.platform      = Gem::Platform::RUBY
  spec.authors       = ['Maciej Mensfeld', 'Pavlo Vavruk']
  spec.email         = %w( maciej@mensfeld.pl pavlo.vavruk@gmail.com )
  spec.homepage      = ''
  spec.summary       = %q{ }
  spec.description   = %q{ }

  spec.add_dependency 'bundler'
  spec.add_dependency 'rake'
  spec.add_dependency 'aspector'
  spec.add_dependency 'poseidon'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(spec)/}) }
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = %w( lib )
end
