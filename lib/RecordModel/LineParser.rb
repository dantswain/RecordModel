require 'RecordModel/RecordModel'

class RecordModel::LineParser
  require 'thread'

  def self.import(io, db, item_class, array_item_class, line_parse_descr, array_sz=2**22,
    report_failures=false, report_progress_every=1_000_000, &block)

    parser = new(db, item_class, array_item_class, line_parse_descr, array_sz,
                 report_failures, report_progress_every)
    parser.start
    res = parser.import(io, &block)
    parser.start
    return res
  end

  def initialize(db, item_class, array_item_class, line_parse_descr, array_sz=2**22,
    report_failures=false, report_progress_every=1_000_000, threaded=true)
    @db = db
    @item_class = item_class
    @item = @item_class.new
    @array_item_class = array_item_class
    @line_parse_descr = line_parse_descr
    @report_failures = report_failures
    @report_progress_every = report_progress_every

    @lines_read, @lines_ok = 0, 0

    @threaded = threaded

    if @threaded
      @inq, @outq = Queue.new, Queue.new
      # two arrays so that the log line parser and DB insert can work in parallel
      2.times { @outq << @array_item_class.make_array(array_sz, false) }
    else
      @arr = @array_item_class.make_array(array_sz, false)
    end
  end

  def start
    return if not @threaded # nothing to do for !@threaded
    raise unless @outq.size == 2
    raise unless @inq.size == 0
    raise if @thread
    @thread = start_db_thread(@inq, @outq) 
  end

  def stop
    if @threaded
      # Remove all packets from @outq and send it back into @inq to be processed
      # in case there are some records left.
      (1..2).map { @outq.pop }.each {|packet| @inq << packet }
      @inq << :end
      @thread.join
      @thread = nil
    else
      store_packet(@arr)
      @arr.reset
    end
  end

  #
  # Method import has to be used together with start() and stop().
  #
  def import(io, &block)
    line_parse_descr = @line_parse_descr
    report_failures = @report_failures
    report_progress_every = @report_progress_every

    item = @item

    arr = process(nil)
    lines_read = @lines_read
    lines_ok = @lines_ok

    while line = io.gets
      lines_read += 1
      begin
        if arr.full?
          arr = process(arr)
        end

        item.zero!
        error = item.parse_line(line, line_parse_descr)
        if new_item = convert_item(error, item)
          arr << new_item
          lines_ok += 1
        end
      rescue 
        if block and report_failures
	  block.call(:failure, [$!, line])
        end
      end # begin .. rescue
      if block and report_progress_every and (lines_read % report_progress_every) == 0
        block.call(:progress, [lines_read, lines_ok])
      end
    end # while

    final(arr)

    diff_lines_read = lines_read - @lines_read 
    diff_lines_ok = lines_ok - @lines_ok
    @lines_read = lines_read
    @lines_ok = lines_ok

    return diff_lines_read, diff_lines_ok 
  end

  def import_fast(io, max_line_len=4096, &block)
    item = @item

    arr = process(nil)

    lines_ok = 0
    lines_read = 0

    loop do
      raise unless arr.empty?
      more, lines = arr.bulk_parse_line(item, io.to_i, @line_parse_descr, max_line_len, &block)
      lines_read += lines
      lines_ok += arr.size
      break unless more

      arr = process(arr)
    end
    final(arr)

    @lines_read += lines_read
    @lines_ok += lines_ok

    return lines_read, lines_ok 
  end

  def process(packet)
    if @threaded
      @inq << packet if packet
      @outq.pop
    else
      if packet
        store_packet(packet)
        packet.reset
        packet
      else
        @arr
      end
    end
  end

  def final(packet)
    if @threaded
      @outq << packet
    else
      raise unless packet == @arr
    end
  end

  protected

  def convert_item(error, item)
    raise if error and error != -1
    return item
  end

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
