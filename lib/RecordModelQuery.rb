class RecordModelQuery
  def initialize(db, klass, *queries)
    @db = db
    @klass = klass
    @queries = queries
    @ranges = queries.map {|q| klass.build_query(q)}
  end

  def each(&block)
    item = @klass.new
    @ranges.each {|from, to| @db.query_each(from, to, item, &block)}
  end

  def to_a
    arr = []
    each {|item| arr << item.dup}
    arr
  end

  def count
    cnt = 0
    each { cnt += 1 }
    cnt
  end

  def into(itemarr=nil)
    item = @klass.new()
    itemarr ||= @klass.make_array(1024)
    if @db.query_into(from, to, item, itemarr)
      return itemarr
    else
      raise "query_into failed"
    end
  end

  def min
    item = @klass.new()
    return @db.query_min(from, to, item)
  end
end
