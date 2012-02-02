require 'RecordModel/RecordModel'
require 'thread'

class RecordModel::LineParser

  def initialize(db, item_class, array_item_class, line_parse_descr, sep=' ', array_sz=2**22,
    report_failures=false, report_progress_every=1_000_000, threaded=true)
    @db = db
    @item_class = item_class
    @item = @item_class.new
    @array_item_class = array_item_class
    @line_parse_descr = line_parse_descr
    @sep = sep
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
    sep = @sep

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
        error = item.parse_line(line, line_parse_descr, sep)
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

  def final(packet)
    if @threaded
      @outq << packet
    else
      raise unless packet == @arr
    end
  end

  protected

  def convert_item(error, item)
    raise if error < @line_parse_descr.size
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

class RecordModel::DbBackgroundInserter
  def initialize(db)
    @db = db
    @thread = nil
  end

  def set_queues(work_q, free_q)
    @work_q, @free_q = work_q, free_q
  end

  def start
    raise if @thread
    @thread = Thread.new { run() }
  end

  def stop
    raise unless @thread
    @thread.join
    @thread = nil
  end

  protected

  def run
    loop do
      packet = @work_q.pop
      break if packet == :end
      store_packet(packet)
      packet.reset
      @free_q << packet
    end
  end

  def fixup_packet(packet)
  end

  def store_packet(packet)
    begin
      fixup_packet(packet)
      @db.put_bulk(packet)
    rescue
      p $!
    end
  end
end

class RecordModel::FastLineParser

  def initialize(inserter, item_class, parser_hash={}, array_sz=2**22)
    initialize_parser(parser_hash)

    @item_class = item_class
    @item = @item_class.new

    @work_q = Queue.new
    @free_q = Queue.new

    # two arrays so that the log line parser and DB insert can work in parallel
    2.times { @free_q << @item_class.make_array(array_sz, false) }

    # The array we are currently putting items in
    @current_arr = nil

    @inserter.set_queues(@work_q, @free_q)
  end

  def initialize_parser(h)
    unless (h.keys - [:line_parse_descr, :reject_token_parse_error, :reject_invalid_num_tokens, :valid_token_range]).empty?
      raise ArgumentError, "wrong keys specified"
    end
    @line_parse_descr = h[:line_parse_descr] || (raise ArgumentError)
    @sep = h[:sep] || ' '
    @reject_token_parse_error = h[:reject_token_parse_error] || true
    @reject_invalid_num_tokens = h[:reject_invalid_num_tokens] || true
    @valid_token_range = h[:valid_token_range] || (@line_parse_descr.size .. -1) 
  end

  def start
    raise unless @free_q.size == 2
    raise unless @work_q.size == 0
    @inserter.start
  end

  def stop
    if @current_arr
      @work_q << @current_arr
      @current_arr = nil
    end
    @work_q << :end

    @inserter.stop
  end

  def with
    start
    begin
      yield
    ensure
      stop
    end
  end

  def import(io, max_line_len=4096, &block)
    lines_ok = 0
    lines_read = 0

    init_work()

    loop do
      before = @current_arr.size
      more, lines = @current_arr.bulk_parse_line(@item, io.to_i, @line_parse_descr, @sep, max_line_len, 
        @reject_token_parse_error, @reject_invalid_num_tokens, @valid_token_range.first, @valid_token_range.last, &block)

      lines_ok += (current_arr.size - before)
      lines_read += lines
      break unless more

      push_work(nil)
    end

    return lines_read, lines_ok 
  end

  protected

  def init_work
    if @current_arr.nil?
      @current_arr = @free_q.pop
    end
  end

  def push_work(item=nil)
    if @current_arr.full?
      @work_q << @current_arr
      @current_arr = @free_q.pop
    end
    @current_arr << item if item
  end
end
