require 'RecordModelExt'

class RecordModel

  class Builder

    #
    # An array of:
    #
    #   [id, type, is_key, offset, length, optional_default_value]
    #
    # entries.
    #
    attr_reader :fields

    def initialize
      @fields = []
      @current_offset = 0
      yield self
    end

    def key(id, type, hash={})
      field(id, type, true, hash)
    end

    def val(id, type, hash={})
      field(id, type, false, hash)
    end

    private

    def field(id, type, is_key, hash)
      size = type_size(type, hash[:size]) 
      _add_field(id.to_sym, type, is_key, @current_offset, size, hash[:default])
      @current_offset += size
    end

    def _add_field(id, type, is_key, offset, length, default_value)
      raise if @fields.assoc(id)
      spec = [id, type, is_key, offset, length]
      spec << default_value if default_value
      @fields << spec
    end

    def type_size(type, sz)
      size = case type
      when :uint64 then 8
      when :uint32 then 4
      when :uint16 then 2
      when :uint8  then 1
      when :timestamp then 8
      when :timestamp_desc then 8
      when :double then 8
      when :hexstr then sz 
      when :string then sz 
      else
        raise
      end
      raise if sz and size != sz
      raise if size > 255
      return size
    end
  end

  def self.define(&block)
    b = Builder.new(&block)

    model = new(b.fields)
    klass = model.to_class
    fields = b.fields
    fields.freeze

    fields.each_with_index do |fld, i|
      id = fld.first
      klass.class_eval "def #{id}() self[#{i}] end" 
      klass.class_eval "def #{id}=(v) self[#{i}] = v end" 
    end

    klass.const_set(:INFO, fields)
    klass.class_eval "def self.__info() INFO end"
    klass.class_eval "def __info() INFO end"

    return klass
  end
end

class RecordModelInstance
  include Comparable

  # XXX: check offset! offsetof macro. default value.
  def self.to_c_struct(name=nil, out="")
    name ||= self.name
    raise unless name

    out << "#ifndef __#{name}__HEADER__\n"
    out << "#define __#{name}__HEADER__\n"

    __info().each_with_index {|arr, i|
      id = arr.first
      out << "#define FLD_#{name}_#{id} #{i}\n"
    }
    out << "#pragma pack(push, 1)\n"
    out << "struct #{name}\n{\n"
    __info().each {|id, type, is_key, offset, length, opt_def|
      out << "  "; out << to_c_type(type, id.to_s, length); out << ";\n"
    }
    out << "};\n"
    out << "#pragma pack(pop)\n"
    out << "#endif\n"
    return out
  end

  def self.write_c_struct(name=nil, filename=nil)
    name ||= self.name
    raise unless name
    filename ||= name + ".h"

    File.write(filename, to_c_struct(name))
  end

  def self.to_c_type(type, name, sz)
    case type
    when :uint64 then 'uint64_t %s'
    when :uint32 then 'uint32_t %s'
    when :uint16 then 'uint16_t %s'
    when :uint8  then 'uint8_t %s'
    when :timestamp then 'uint64_t %s'
    when :timestamp_desc then 'uint64_t %s'
    when :double then 'double %s'
    when :hexstr then "char %s[#{sz}]"
    when :string then "char %s[#{sz}]" 
    else
      raise
    end % name
  end

  alias old_initialize initialize

  def initialize(hash=nil)
    old_initialize()
    hash.each {|k, v| self.send(:"#{k}=", v) } if hash
  end

  def set_min(fld_idx=nil)
    _set_min(fld_idx)
  end

  def set_max(fld_idx=nil)
    _set_max(fld_idx)
  end

  def to_hash
    h = {}
    __info().each {|fld| id = fld.first; h[id] = send(id)}
    h
  end

  def _to_hash(is_key)
    h = {}
    __info().each {|fld|
      if fld[2] == is_key
        id = fld.first
        h[id] = send(id)
      end
    }
    h
  end

  def keys_to_hash
    _to_hash(true)
  end

  def values_to_hash
    _to_hash(false)
  end

  def inspect
    [self.class, keys_to_hash(), values_to_hash()]
  end

  def self.make_array(n, expandable=true)
    RecordModelInstanceArray.new(self, n, expandable)
  end

  def self.build_query(query)
    from = new().set_min
    to = new().set_max

    query.each {|id, q|
      idx = sym_to_fld_idx(id)
      case q
      when Range 
        raise ArgumentError if q.exclude_end?
        from[idx] = q.first 
        to[idx] = q.last
      else
        from[idx] = to[idx] = q
      end
    }

    return from, to
  end

  #
  # Example usage: def_parse_descr(:uid, :campaign_id, nil, :timestamp)
  #
  def self.def_parse_descr(*args)
    args.map {|arg|
      case arg 
      when nil
        nil # skip
      when Symbol
        sym_to_fld_idx(arg)
      else
        raise ArgumentError
      end
    }
  end

  def self.sym_to_fld_idx(sym)
    __info().index {|fld| fld.first == sym} || raise
  end

  def sym_to_fld_idx(sym)
    __info().index {|fld| fld.first == sym} || raise
  end

end

class RecordModelInstanceArray
  attr_reader :model_klass

  alias old_initialize initialize

  def initialize(model_klass, n=16, expandable=true)
    @model_klass = model_klass
    old_initialize(model_klass, n, expandable)
  end

  include Enumerable

  def each
    instance = @model_klass.new
    _each(instance) {|i| yield i.dup}
  end

  def update_each(attr, matching_value, instance=nil, &block)
    instance ||= @model_klass.new
    _update_each(@model_klass.sym_to_fld_idx(attr), matching_value, instance, &block)
  end

  alias old_bulk_set bulk_set
  def bulk_set(attr, value)
    old_bulk_set(@model_klass.sym_to_fld_idx(attr), value)
  end

  def inspect
    [self.class, to_a]
  end

  def sort(arr=nil)
    if arr
      _sort(arr.map{|attr| @model_klass.sym_to_fld_idx(attr)})
    else
      _sort(arr)
    end
  end
end
