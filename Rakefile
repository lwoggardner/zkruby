# -*- ruby -*-

require 'rubygems'
require 'hoe'
require './yard_ext/enum_handler'
require './jute/lib/jute.rb'
require './jute/lib/hoe.rb'


# Hoe.plugin :compiler
# Hoe.plugin :gem_prelude_sucks
Hoe.plugin :git
# Hoe.plugin :inline
# Hoe.plugin :racc
# Hoe.plugin :rubyforge
Hoe.plugin :yard
Hoe.plugin :jute

Hoe.spec 'zkruby' do
  self.readme_file="README.rdoc"
  developer('Grant Gardner', 'grant@lastweekend.com.au')
  extra_deps << [ 'slf4r' , '>= 0.4.2' ]
  
  self.jute_modules = {
      "org.apache.zookeeper.data" => "ZooKeeper::Data",
      "org.apache.zookeeper.proto" => "ZooKeeper::Proto"}
end

# vim: syntax=ruby
