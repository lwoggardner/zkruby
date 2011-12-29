# -*- ruby -*-
# hoe  2.12.5 goes looking for the plugins so we have to do it this way..
$LOAD_PATH.unshift File.dirname(__FILE__) + '/jute/lib'

require 'rubygems'
require 'hoe'
require './yard_ext/enum_handler'


# Hoe.plugin :compiler
#Hoe.plugin :gem_prelude_sucks
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
