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
    info = {} 
    defs = {}
    defs_x2 = []

    keys = []
    b.keys.each do |id, type| 
      info[id] = :key
      if type == :uint64_x2
        desc = (offset << 16) | TYPES[:uint64]
        offset += TYPES[:uint64] & 0xFF
        keys << desc
        defs[:"#{id}__0"] = desc

        desc = (offset << 16) | TYPES[:uint64]
        offset += TYPES[:uint64] & 0xFF
        keys << desc
        defs[:"#{id}__1"] = desc

        defs_x2 << id
      else
        desc = (offset << 16) | TYPES[type]
        offset += TYPES[type] & 0xFF
        keys << desc
        defs[id] = desc
      end
    end

    values = []
    b.values.each do |id, type| 
      info[id] = :value
      if type == :uint64_x2
        desc = (offset << 16) | TYPES[:uint64]
        offset += TYPES[:uint64] & 0xFF
        values << desc
        defs[:"#{id}__0"] = desc

        desc = (offset << 16) | TYPES[:uint64]
        offset += TYPES[:uint64] & 0xFF
        values << desc
        defs[:"#{id}__1"] = desc

        defs_x2 << id
      else
        desc = (offset << 16) | TYPES[type]
        offset += TYPES[type] & 0xFF
        values << desc
        defs[id] = desc
      end
    end

    model = new(keys, values)
    klass = model.to_class

    defs.each do |id, desc|
      klass.class_eval "def #{id}() self[#{desc}] end" 
      klass.class_eval "def #{id}=(v) self[#{desc}] = v end" 
    end

    defs_x2.each do |id|
      klass.class_eval "def #{id}() (#{id}__0 << 64) | #{id}__1 end"
      klass.class_eval %[
        def #{id}=(v)
          self.#{id}__0 = (v >> 64) & 0xFFFFFFFF_FFFFFFFF
          self.#{id}__1 = (v)       & 0xFFFFFFFF_FFFFFFFF
        end]
    end

    info.freeze
    klass.const_set(:INFO, info)
    klass.class_eval "def __info() INFO end"

    return klass
  end

  TYPES = {
    :uint64 => 0x0008,
    :uint32 => 0x0004,
    :uint16 => 0x0002,
    :uint8 => 0x0001,
    :double => 0x0108
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
    __info().each {|id,t| h[id] = send(id) if t == :key}
    h
  end

  def values_to_hash
    h = {}
    __info().each {|id,t| h[id] = send(id) if t == :value}
    h
  end

  def inspect
    [self.class, keys_to_hash, values_to_hash]
  end
end
