module Enumeration

  def === (other)
    case other
     when Enumeration
        to_sym == other.to_sym
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

  def to_s
    "#{super}(:#{@name} [#{@index}])"
  end

  def enum_exception(*args)
    dup().orig_exception(*args)
  end

  def self.included(base)
    base.extend(ClassMethods)
    
    if (base.method_defined?(:exception))
        base.send(:alias_method,:orig_exception,:exception)
        base.send(:alias_method,:exception,:enum_exception)
    end
  end

  module ClassMethods
    def get(ref)
        ref.kind_of?(Enumeration) ? ref : @enums[ref]
    end

    def fetch(ref)
        ref.kind_of?(Enumeration) ? ref : @enums.fetch(ref)
    end

    def lookup(ref)
        case ref
        when Enumeration 
            ref
        when Symbol
            get(ref) || enum(ref,nil)
        when Fixnum
            get(ref) || enum(nil,ref)
        else
            nil
        end
    end

    def enum(name,index,*args)
        @enums ||= {}
        #TODO: This causes problems for JRuby
        instance = self.new(*args)
        @enums[name] = instance if name
        @enums[index] = instance if index
        instance.instance_variable_set(:@name,name)
        instance.instance_variable_set(:@index,index)
        instance.freeze
    end

    def const_missing(key)
      @enums[key.downcase] || super
    end    

    def method_missing(method,*args)
      @enums[method.downcase] || super
    end

  end
end


