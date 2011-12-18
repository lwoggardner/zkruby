# coding: utf-8

module Hoe::Jute
   attr_accessor :jute_tasks
   attr_accessor :jute_modules
   #attr_accessor :jute_compiler
   def initialize_jute
        jute_tasks = [:test,:spec]
        dependency 'citrus', '~> 2.4.0', :development
        #dependency 'jute' # if jute is ever a separate gem
        dependency 'bindata', '~> 1.4.1'
   end

   def define_jute_tasks
         jute_compiler = Jute::Compiler.new()
         jute_files   = self.spec.files.find_all { |f| f =~ /\.jute$/ }

         record_files = jute_files.map { |f| f.pathmap("%{src,lib}X.rb") }

         self.clean_globs += record_files

          rule ".rb" => ["%{lib,src}X.jute"] do |t|
              File.open(t.source) do |input|
                File.open(t.name,"w") do |output|
                    jute_compiler.compile(input,output,jute_modules)
                end
              end
          end

          desc "generate jute records" unless jute_files.empty?
          task :compile_jute

          task :compile_jute => record_files

          jute_tasks.each do |t|
            task t => [:compile_jute]
          end 
   end

end


