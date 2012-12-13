#!/usr/bin/env rake
$:.unshift "jute/lib"

require 'rake/clean'
require 'yard'
require './yard_ext/enum_handler.rb'
#require "bundler/gem_tasks"
require 'rspec/core/rake_task'
require 'jute/task'

RSpec::Core::RakeTask.new
YARD::Rake::YardocTask.new 

Jute::Task.new() do |t|
  t.modules = {
      "org.apache.zookeeper.data" => "ZooKeeper::Data",
      "org.apache.zookeeper.proto" => "ZooKeeper::Proto"}
end


task :spec => :jute
task :build => :jute
task :install => :jute
task :release => :jute
task :yard => :jute

task :default => [:spec,:yard]

CLEAN.include "*.out","Gemfile.lock",".yardoc/"
CLOBBER.include "doc/","pkg/","lib/jute"
