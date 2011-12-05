require 'RecordModelLevelDBExt'

class RecordModelLevelDB
  class << self
    alias _open open
    def open(db, modelklass, &block)
      if block
        begin
          db = _open(db, modelklass)
          block.call(db)
        ensure
          db.close
        end
      else
        _open(db, modelklass)
      end
    end 
  end
end
