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

  def self.build_query(query={})
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

  def self.db_query(db, query={}, &block)
    from, to = build_query(query)
    item = new()
    db.query(from, to, item, &block)
  end

  def self.db_query_into(db, itemarr=nil, query={})
    from, to = build_query(query)
    item = new()
    itemarr ||= make_array(1024)
    if db.query_into(from, to, item, itemarr)
      return itemarr
    else
      raise "query_into failed"
    end
  end

  #
  # Example usage: def_parser_descr(:uid, :campaign_id, nil, [:timestamp, :fixint, 3])
  #
  def self.def_parse_descr(*args)
    args.map {|arg|
      case arg 
      when nil
        nil # skip
      when Symbol
        idx = __info().index {|fld| fld.first == arg}
        idx || raise
      when Array
        id, type, extra = *arg
        idx = __info().index {|fld| fld.first == id} || raise
        if type == :fixint
          (((extra << 8) | 0x01) << 32) | idx
        else
          raise ArgumentError
        end
      else
        raise ArgumentError
      end
    }
  end

end

class RecordModelInstanceArray
  alias old_initialize initialize

  attr_reader :model_klass

  def initialize(model_klass, n=16, expandable=true)
    @model_klass = model_klass
    old_initialize(model_klass, n, expandable)
  end

  alias old_each each
  def each(instance=nil, &block)
    old_each(instance || @model_klass.new, &block)
  end

  def inspect
    [self.class, to_a]
  end

  def to_a
    a = []
    each {|i| a << i.dup}
    a
  end
end


class RecordModel::LineParser
  require 'thread'

  def initialize(db, item_class, array_item_class, line_parse_descr)  
    @db = db
    @item_class = item_class
    @array_item_class = array_item_class
    @line_parse_descr = line_parse_descr
  end

  def convert_item(error, item)
    raise if error and error != -1
    return item
  end

  def import(io, array_sz=2**22, report_failures=false, report_progress_every=1_000_000, &block)
    line_parse_descr = @line_parse_descr

    inq, outq = Queue.new, Queue.new
    thread = start_db_thread(inq, outq) 

    # two arrays so that the log line parser and DB insert can work in parallel
    2.times { outq << @array_item_class.make_array(array_sz, false) }

    item = @item_class.new

    arr = outq.pop
    lines_read = 0
    lines_ok = 0

    while line = io.gets
      lines_read += 1
      begin
        item.zero!
        error = item.parse_line(line, line_parse_descr)
        if new_item = convert_item(error, item)
          arr << new_item
          lines_ok += 1
        end
        if arr.full?
          inq << arr 
	  arr = outq.pop
        end
      rescue 
        if report_failures and block
	  block.call(:failure, [$!, line])
        end
      end # begin .. rescue
      if report_progress_every and (lines_read % report_progress_every) == 0 and block
        block.call(:progress, [lines_read, lines_ok])
      end
    end # while

    inq << arr
    inq << :end

    thread.join

    return lines_read, lines_ok
  end

  protected

  def store_packet(packet)
    begin
      @db.put_bulk(packet)
    rescue
      p $!
    end
  end

  def start_db_thread(inq, outq)
    Thread.new {
      loop do
        packet = inq.pop
        break if packet == :end
        store_packet(packet)
        packet.reset
        outq << packet
      end
    }
  end
end
