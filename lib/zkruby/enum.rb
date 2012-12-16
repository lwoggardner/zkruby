
# Including in a class and us the "enum" DSL method to create class instances
# As a special case, if the including class is descendant from StandardError, the enumerated
# instances will be classes descendent from the including class (eg like Errno)
module Enumeration

    def self.included(base)
        base.extend(ClassMethods)
        #  In the normal case we just include our methods in the base class
        #  but for Errors we need to include them on each instance
        base.send :include,InstanceMethods unless base < StandardError
    end

    module InstanceMethods
        def === (other)
            case other
            when Symbol
                to_sym == other
            when Fixnum
                to_int == other
            else
                super
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
            "#{super} (:#{@name} [#{@index}])"
        end
    end

    module ClassMethods
      
        # @param [Symbol,Fixnum,Enumeration] ref 
        # @return [Enumeration] instance representing ref or nil if not found
        def get(ref)
            @enums[ref]
        end

        # @param [Symbol,Fixnum] ref 
        # @raise [KeyError] if ref not found
        # @return [Enumeration] 
        def fetch(ref)
            @enums.fetch(ref)
        end

        # Will dynamically create a new enum instance if ref is not found
        def lookup(ref)
            case ref
            when Enumeration,Class 
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
            
            instance = if (self < StandardError) then Class.new(self) else self.new(*args) end
            
            instance.extend(InstanceMethods) if (self < StandardError)

            const_name = if name.nil?
                             "ENUM" + index.to_s.sub("-","_")
                         else
                             name.to_s.upcase
                         end
                            
            self.const_set(const_name.to_s.upcase,instance)
            
            @enums[instance] = instance
            @enums[name] = instance if name
            @enums[index] = instance if index
            instance.instance_variable_set(:@name,name)
            instance.instance_variable_set(:@index,index)
            instance.freeze
        end

        def method_missing(method,*args)
            @enums[method.downcase] || super
        end

    end
end
