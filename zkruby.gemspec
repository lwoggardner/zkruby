# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

require "zkruby/version"

Gem::Specification.new do |s|
  s.name        = "zkruby"
  s.version     = ZooKeeper::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Grant Gardner"]
  s.email       = ["grant@lastweekend.com.au"]
  s.homepage    = "http://rubygems.org/gems/zkruby"
  s.summary     = %q{Pure Ruby language binding for ZooKeeper}
  s.description = %q{Supports full ZooKeeper API, synchronous or asynchronous style, watches etc.. with implementations over EventMachine or plain old Ruby IO/Threads}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {spec}/*`.split("\n")
  s.require_paths = ["lib"]

  s.has_rdoc = 'yard'
  # Yard options in .yardopts

  s.add_dependency 'slf4r' , '~> 0.4.2'
  s.add_dependency 'strand', '~> 0.2.0.rc0'
  s.add_dependency 'bindata', '~> 1.4.1'

  s.add_development_dependency 'eventmachine', '>= 0.12.10'
  s.add_development_dependency 'logging', '>= 1.4.1'

  s.add_development_dependency("rake")
  s.add_development_dependency("rspec")
  s.add_development_dependency("yard")
 
  # s.add_development_dependency("jute") 
  s.add_development_dependency "citrus" , '~> 2.4.0'
 
end
