# -*- ruby -*-
# hoe  2.12.5 goes looking for the plugins so we have to do it this way..
$LOAD_PATH.unshift File.dirname(__FILE__) + '/jute/lib'

require 'rubygems'
require 'hoe'

begin
    require './yard_ext/enum_handler'
rescue LoadError => err
     warn "%p while trying to load yard extensions: %s" % [ err.class, err.message ]
end
    


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
  dependency 'slf4r' , '~> 0.4.2' 
  dependency 'eventmachine', '~> 0.12.10', :development 
  dependency 'strand', '~> 0.1.0', :development 
  dependency 'logging', '>= 1.4.1', :development
  dependency 'rspec', '>=2.7.0', :development
  dependency 'hoe-yard', '>=0.1.2', :development

  self.jute_modules = {
      "org.apache.zookeeper.data" => "ZooKeeper::Data",
      "org.apache.zookeeper.proto" => "ZooKeeper::Proto"}
end
# vim: syntax=ruby
