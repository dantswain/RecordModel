require 'MMDB/DB'
require 'MMDB/CommitLog'

module MMDB

  class DBMS
    def self.open(*args, &block)
      new(*args, &block)
    end

    def initialize(dirname, readonly, schemas)
      @dirname = dirname
      @readonly = readonly
      @schemas = schemas
      raise ArgumentError if File.exist?(@dirname) && !File.directory?(@dirname)
      Dir.mkdir(@dirname) unless File.exist?(@dirname)

      # XXX: Validate Schema
      #if File.exist?(File.join(@dirname, "schema"))

      @commit_log = CommitLog.new(File.join(@dirname, "commit"))

      # parse the commit record
      cr = {}
      if str = @commit_log.last
        logr = str.split(",").map {|i| Integer(i)}
        raise "invalid commit log entry" if logr.size != (2*@schemas.size+1)
        @external_state = logr.shift 
        @schemas.each {|arr| id = arr.first; cr[id] = logr.shift(2)}
        raise unless logr.empty? 
      else
        @external_state = 0
        @schemas.each {|arr| id = arr.first; cr[id] = [0, 0]}
      end

      @dbs = {}

      @schemas.each do |arr|
        id, klass, hint1, hint2 = *arr
        raise ArgumentError unless id.is_a?(Symbol)
        raise ArgumentError unless klass
        raise ArgumentError if @dbs[id]
        hint0 ||= 1024 
        hint1 ||= 1024*1024
        db = DB.open(klass, File.join(@dirname, "db_#{id}_"), cr[id][0], hint0, cr[id][1], hint1, @readonly)
        raise "Cannot open a database" unless db
        @dbs[id] = db
      end
    end

    def with
      begin
        yield self
      ensure
        close
      end
    end

    def put_bulk(dbid, arr)
      raise ArgumentError if @readonly
      get_db(dbid).put_bulk(arr)
    end

    def query(dbid, *args, &block)
      get_db(dbid).query(*args, &block)
    end

    attr_reader :external_state

    def commit(external_state=0)
      raise ArgumentError if @readonly
      raise ArgumentError unless external_state

      logr = [external_state]
      @schemas.each {|arr|
        id = arr.first
        ok = get_db(id).commit
        raise unless ok
        num_slices, num_records = *ok
        logr << num_slices
        logr << num_records
      }
      raise "invalid commit log entry" if logr.size != (2*@schemas.size+1)

      @commit_log.append(logr.join(","))

      @external_state = external_state

      return logr
    end
    
    #
    # XXX: No writing should occur during snapshot generation
    #
    def snapshot
      snap = {}
      @dbs.each {|id, db| snap[id] = db.snapshot}
      DBMS::Snapshot.new(snap)
    end

    def close
      @dbs.each_value {|db| db.close}
      @dbs = {}
    end

    def get_db(dbid)
      db = @dbs[dbid]
      raise ArgumentError, "#{dbid} not a valid DB" unless db
      return db
    end

    alias [] get_db
  end

  class DBMS::Snapshot
    def initialize(dbs)
      @dbs = dbs
    end

    def snapshot
      self
    end

    def query(dbid, *args, &block)
      get_db(dbid).query(*args, &block)
    end

    def get_db(dbid)
      db = @dbs[dbid]
      raise ArgumentError, "#{dbid} not a valid DB" unless db
      return db
    end

    alias [] get_db
  end

end
