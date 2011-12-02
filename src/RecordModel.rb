require 'RecordModelExt'

class RecordModel

  class Builder
    attr_accessor :keys, :values
    def initialize
      @keys = []
      @values = []
      yield self
    end

    def key(id, type)
      @keys << [id, type]
    end

    def val(id, type)
      @values << [id, type]
    end
  end

  def self.define(&block)
    b = Builder.new(&block)

    offset = 0
    h = {}

    keys = b.keys.map do |id, type| 
      desc = (offset << 16) | TYPES[type]
      h[id] = desc
      offset += TYPES[type] & 0xFF
      desc
    end

    values = b.values.map do |id, type| 
      desc = (offset << 16) | TYPES[type]
      h[id] = desc
      offset += TYPES[type] & 0xFF
      desc
    end

    model = new(keys, values)
    klass = model.to_class
    h.each do |id, desc|
      klass.class_eval "def #{id}() self[#{desc}] end" 
      klass.class_eval "def #{id}=(v) self[#{desc}] = v end" 
    end

    return klass
  end

  TYPES = {
    :uint64 => 0x0008#,
    #:uint32 => 0x0004
  }

end
