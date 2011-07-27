module Enumeration
  

  def == (other)
     case other
     when Symbol
        to_sym == other
     when Fixnum
        to_int == other
     else
        super(other)
     end
  end

  def to_i
    to_int
  end

  def to_int
    @index
  end

  def |(num)
    to_int | num
  end
  
  def &(num)
    to_int & num
  end

  def to_sym
    @name
  end
 
  def self.included(base)
    base.extend(ClassMethods)    
  end
 
  module ClassMethods
    def get(ref)
        ref.kind_of?(Enumeration) ? ref : @enums[ref]
    end

    def fetch(ref)
        ref.kind_of?(Enumeration) ? ref : @enums.fetch(ref)
    end

    def enum(name,index,*args)
        @enums ||= {}
        #TODO: This causes problems for JRuby
        instance = self.new(*args)
        @enums[name] = instance
        @enums[index] = instance
        instance.instance_variable_set(:@name,name)
        instance.instance_variable_set(:@index,index)
    end

    def const_missing(key)
      @enums[key.downcase]
    end    
 
  end
end


