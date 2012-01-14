require 'RecordModelExt'

class RecordModel

  class Builder

    #
    # An array of:
    #
    #   [id, type, is_key, offset, length]
    #
    # entries.
    #
    attr_reader :fields

    def initialize
      @fields = []
      @current_offset = 0
      yield self
    end

    def key(id, type, sz=nil)
      field(id, type, sz, true)
    end

    def val(id, type, sz=nil)
      field(id, type, sz, false)
    end

    private

    def field(id, type, sz, is_key)
      size = type_size(type, sz) 
      _add_field(id.to_sym, type, is_key, @current_offset, size)
      @current_offset += size
    end

    def _add_field(id, type, is_key, offset, length)
      raise if @fields.assoc(id)
      @fields << [id, type, is_key, offset, length]
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
      else
        raise
      end
      raise if sz and size != sz
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

  alias old_initialize initialize

  def initialize(hash=nil)
    old_initialize()
    hash.each {|k, v| self.send(:"#{k}=", v) } if hash
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
    from = new()
    to = new()

    used_keys = [] 

    __info().each_with_index {|fld, idx|
      next unless fld[2] # we only want keys!
      id = fld.first
 
      if query.has_key?(id)
        used_keys << id
        case (q = query[id])
        when Range 
          raise ArgumentError if q.exclude_end?
          from[idx] = q.first 
          to[idx] = q.last
        else
          from[idx] = to[idx] = q
        end
      else
        from.set_min(idx)
        to.set_max(idx)
      end
    }

    raise ArgumentError unless (query.keys - used_keys).empty?

    return from, to
  end

  #
  # Example usage: def_parser_descr(:uid, :campaign_id, nil, :timestamp)
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

  alias old_bulk_set bulk_set
  def bulk_set(attr, value)
    old_bulk_set(@model_klass.sym_to_fld_idx(attr), value)
  end

  def inspect
    [self.class, to_a]
  end
end
