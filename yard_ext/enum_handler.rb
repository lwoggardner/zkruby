require 'yard'


class EnumConstantHandler < YARD::Handlers::Ruby::ConstantHandler
  # Bug in YARD means negative constants are not shown properly.
  handles method_call(:enum)
  namespace_only
  def process
    name = statement.parameters.first.jump(:ident).source
    value = statement.parameters[1].source
    register YARD::CodeObjects::ConstantObject.new(namespace, name.upcase) { |o|
        o.source = statement; o.value = value; o.dynamic=true
    }

  end
end
