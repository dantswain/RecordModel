require 'RecordModelMMDBExt'
require 'RecordModelQuery'

class RecordModelMMDB
  alias _snapshot snapshot

  # Redefine snapshot method
  def snapshot
    RecordModelMMDB::Snapshot.new(self, _snapshot)
  end

  def query(klass, *queries)
    RecordModelQuery.new(self.snapshot, klass, *queries)
  end
end

class RecordModelMMDB::Snapshot
  def initialize(db, snapshot)
    @db, @snapshot = db, snapshot
  end

  def snapshot
    self
  end

  def _snapshot
    @snapshot
  end

  def query(klass, *queries)
    RecordModelQuery.new(self, klass, *queries)
  end

  def query_each(from, to, item, &block)
    @db.query_each(from, to, item, @snapshot, &block)
  end

  def query_into(from, to, item, itemarr)
    @db.query_into(from, to, item, itemarr, @snapshot)
  end

  def query_min(from, to, item)
    @db.query_min(from, to, item, @snapshot)
  end
end
