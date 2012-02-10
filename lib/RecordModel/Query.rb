require 'RecordModel/RecordModel'

class RecordModel::Query
  def initialize(db, klass, *queries)
    @db = db
    @klass = klass
    if queries.empty?
      @queries = [{}]
    else
      @queries = queries
    end
    @ranges = @queries.map {|q| klass.build_query(q)}
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
    item = @klass.new
    cnt = 0
    @ranges.each {|from, to| cnt += @db.query_count(from, to, item)}
    cnt
  end

  def aggregate(fields, itemarr=nil, sum=true)
    fields = fields.map {|field| @klass.sym_to_fld_idx(field) }
    itemarr ||= @klass.make_array(1024) # should be expandable!
    item = @klass.new
    @ranges.each {|from, to|
      @db.query_aggregate(from, to, item, itemarr, fields, sum)
    }
    return itemarr
  end

  def into(itemarr=nil)
    item = @klass.new()
    itemarr ||= @klass.make_array(1024)
    @ranges.each {|from, to|
      raise "query_into failed" unless @db.query_into(from, to, item, itemarr)
    }
    return itemarr 
  end

  def min
    min = nil
    item = @klass.new()
    @ranges.each {|from, to|
      if c = @db.query_min(from, to, item)
        if min.nil? or c < min
          min = c.dup
        end
      end
    }
    return min
  end
end
