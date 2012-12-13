# coding: utf-8
require 'rake/tasklib'
require 'jute'

class Jute::Task < Rake::TaskLib

   attr_accessor :modules
   attr_accessor :files
   attr_accessor :pathmap

   def initialize name = :jute
        
        defaults

        @name = name

        yield self if block_given?

        define_jute_tasks
   end

   def defaults
       @files = "src/jute/*.jute"
       @pathmap = "%{src,lib}X.rb"
   end

   def define_jute_tasks
	desc "Compile jute files to ruby classes"
	task jute_task_name

	raise "modules hash must be defined" unless Hash === @modules
        FileList.new(@files).each do | source |
           target = source.pathmap(@pathmap)

           target_dir = target.pathmap("%d")
           directory target_dir

           file target => [source,target_dir] do
                compile_jute(source,target)
           end
           task jute_task_name => target
        end
   end

   def jute_task_name
	@name
   end

   def compile_jute(source,target)
 
      @jute_compiler = ::Jute::Compiler.new() unless @jute_compiler

      File.open(source) do |input|
        File.open(target,"w") do |output|
             puts "Compiling #{input.inspect} to #{output.inspect}"
             @jute_compiler.compile(input,output,modules)
        end
      end
   end
end


