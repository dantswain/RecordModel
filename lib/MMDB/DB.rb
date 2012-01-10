require 'RecordModelMMDBExt'
require 'RecordModel/RecordModel'
require 'RecordModel/Query'

module MMDB

  class DB < RecordModelMMDB

    attr_accessor :modelklass

    def self.open(modelklass, path, *args)
      db = super(modelklass.model, path, *args)
      if db
        db.modelklass = modelklass
      end
      db
    end

    # Redefine snapshot method
    def snapshot
      DB::Snapshot.new(self, get_snapshot_num())
    end

    def query(*queries)
      RecordModel::Query.new(self.snapshot, self.modelklass, *queries)
    end
  end

  class DB::Snapshot
    def modelklass
      @db.modelklass
    end

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
      RecordModel::Query.new(self, self.modelklass, *queries)
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
