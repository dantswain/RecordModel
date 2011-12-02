require 'RecordModelExt'

class RecordModel

  class Builder
    attr_accessor :keys, :values
    def initialize
      @ids = {}
      @keys = []
      @values = []
      yield self
    end

    def key(id, type)
      raise if @ids.has_key?(id)
      @ids[id] = true
      @keys << [id, type]
    end

    def val(id, type)
      raise if @ids.has_key?(id)
      @ids[id] = true
      @values << [id, type]
    end

    alias value val
  end

  def self.define(&block)
    b = Builder.new(&block)

    offset = 0
    info = []

    keys = b.keys.map do |id, type| 
      desc = (offset << 16) | TYPES[type]
      info << [id, desc, type, :key]
      offset += TYPES[type] & 0xFF
      desc
    end

    values = b.values.map do |id, type| 
      desc = (offset << 16) | TYPES[type]
      info << [id, desc, type, :value]
      offset += TYPES[type] & 0xFF
      desc
    end

    model = new(keys, values)
    klass = model.to_class
    info.each do |id, desc,_,_|
      klass.class_eval "def #{id}() self[#{desc}] end" 
      klass.class_eval "def #{id}=(v) self[#{desc}] = v end" 
    end
    info.freeze
    klass.const_set(:INFO, info)
    klass.class_eval "def __info() INFO end"

    return klass
  end

  TYPES = {
    :uint64 => 0x0008,
    :uint32 => 0x0004,
    :double => 0x0108
  }

end

class RecordModelInstance
  def to_hash
    h = {}
    __info().each {|id,_,_,_| h[id] = send(id)}
    h
  end

  def keys_to_hash
    h = {}
    __info().each {|id,_,_,t| h[id] = send(id) if t == :key}
    h
  end

  def values_to_hash
    h = {}
    __info().each {|id,_,_,t| h[id] = send(id) if t == :value}
    h
  end

  def inspect
    [self.class, keys_to_hash, values_to_hash]
  end
end
