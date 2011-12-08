require 'RecordModelExt'

class RecordModel

  class Builder
    attr_accessor :keys, :values, :accs
    def initialize
      @ids = {}
      @keys = []
      @values = []
      @accs = {}
      yield self
    end

    def key(id, type, sz=nil)
      raise if @ids.has_key?(id)
      @ids[id] = true
      @keys << [id, type, sz]
    end

    def val(id, type, sz=nil)
      raise if @ids.has_key?(id)
      @ids[id] = true
      @values << [id, type, sz]
    end

    def acc(id, type, *args)
      case type
      when :uint64_x2
        id0, id1, _ = *args
        raise if _
        @accs[id] = %[
          def #{id}() (self.#{id0} << 64) | self.#{id1} end
          def #{id}=(v)
            self.#{id0} = (v >> 64) & 0xFFFFFFFF_FFFFFFFF
            self.#{id1} = (v)       & 0xFFFFFFFF_FFFFFFFF
          end
        ]
      else
        raise
      end
    end

    alias value val
  end

  def self.define(&block)
    b = Builder.new(&block)

    offset = 0
    info = {} 

    keys = []
    b.keys.each do |id, type, sz|
      desc = def_descr(offset, type, sz)
      offset += type_size(type, sz)
      keys << desc
      info[id] = [desc, :key]
    end

    values = []
    b.values.each do |id, type, sz|
      desc = def_descr(offset, type, sz)
      offset += type_size(type, sz)
      values << desc
      info[id] = [desc, :value]
    end

    model = new(keys, values)
    klass = model.to_class

    info.each do |id, v|
      desc = v[0]
      klass.class_eval "def #{id}() self[#{desc}] end" 
      klass.class_eval "def #{id}=(v) self[#{desc}] = v end" 
    end

    b.accs.each do |id, code|
      klass.class_eval code
    end

    info.freeze
    klass.const_set(:INFO, info)
    klass.class_eval "def self.__info() INFO end"
    klass.class_eval "def __info() INFO end"

    return klass
  end

  def self.def_descr(offset, type, sz=nil)
    (offset << 16) | TYPES[type] | type_size(type, sz)
  end

  def self.type_size(type, sz)
    if TYPES[type] & 0xFF == 0
      raise if sz.nil?
      return sz
    else
      raise if sz and TYPES[type] & 0xFF != sz
      return TYPES[type] & 0xFF
    end
  end

  TYPES = {
    :uint64 => 0x0008,
    :uint32 => 0x0004,
    :uint16 => 0x0002,
    :uint8 => 0x0001,
    :double => 0x0108,
    :hexstr => 0x0200
  }

end

class RecordModelInstance
  def to_hash
    h = {}
    __info().each_key {|id| h[id] = send(id)}
    h
  end

  def keys_to_hash
    h = {}
    __info().each {|id,v| t = v[1]; h[id] = send(id) if t == :key}
    h
  end

  def values_to_hash
    h = {}
    __info().each {|id,v| t = v[1]; h[id] = send(id) if t == :value}
    h
  end

  def inspect
    [self.class, keys_to_hash, values_to_hash]
  end
end