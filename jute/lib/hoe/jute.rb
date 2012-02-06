# coding: utf-8

module Hoe::Jute
   attr_accessor :jute
   attr_accessor :jute_tasks
   attr_accessor :jute_modules

   #attr_accessor :jute_compiler
   def initialize_jute
        self.jute_tasks = [:test,:spec,:package]
        dependency 'citrus', '~> 2.4.0', :development
        #dependency 'jute' # if jute is ever a separate gem
        dependency 'bindata', '~> 1.4.1'
   end

   def define_jute_tasks

      found = try_load_jute()

      if found
         jute_compiler = ::Jute::Compiler.new()
         jute_files   = self.spec.files.find_all { |f| f =~ /\.jute$/ }

         record_files = jute_files.map { |f| f.pathmap("%{src,lib}X.rb") }
         self.clean_globs += record_files


          rule ".rb" => ["%{lib,src}X.jute"] do |t|
              File.open(t.source) do |input|
                File.open(t.name,"w") do |output|
                    puts "compiling #{input.inspect} to #{output.inspect}"
                    jute_compiler.compile(input,output,jute_modules)
                end
              end
          end

          desc "generate jute records" unless jute_files.empty?
          task :jute

          task :jute => record_files

          jute_tasks.each do |t|
            task t => [:jute]
          end
       end
   end

   def try_load_jute()
        require 'jute'
   rescue LoadError => err
        warn "%p while trying to load jute: %s" % [ err.class, err.message ]
        false
   end
end


