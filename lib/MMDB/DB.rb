require 'RecordModelMMDBExt'
require 'RecordModel/RecordModel'
require 'RecordModel/Query'

module MMDB

  class DB < RecordModelMMDB
    # Redefine snapshot method
    def snapshot
      Snapshot.new(self, get_snapshot_num())
    end

    def query(klass, *queries)
      RecordModel::Query.new(self.snapshot, klass, *queries)
    end
  end

  class Snapshot
    def initialize(db, snapshot)
      @db, @snapshot = db, snapshot
    end

    def snapshot
      self
    end

    def get_snapshot_num
      @snapshot
    end

    def query(klass, *queries)
      RecordModel::Query.new(self, klass, *queries)
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

end # module MMDB
