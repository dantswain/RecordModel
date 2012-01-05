require 'test/unit'

$LOAD_PATH << "../ext/RecordModel" 
$LOAD_PATH << "../ext/MMDB" 
$LOAD_PATH << "../lib" 
require 'RecordModel'
require 'RecordModelMMDB'

class TestRecordModelMMDB < Test::Unit::TestCase

  def setup
    @klass = RecordModel.define do |r|
      r.key :a, :uint8
      r.key :b, :uint16
      r.key :c, :uint32
      r.key :d, :uint64
      r.val :e, :double
      r.val :f, :hexstr, 16
      r.key :g, :timestamp
    end
  end

  def test_open
    `mkdir -p ./tmp.test/db`
    db = RecordModelMMDB.open(@klass.model, "./tmp.test/db/", 0, 0, false) 
    db.close
    `rm -rf ./tmp.test/db`
  end

  def test_write
    `rm -rf ./tmp.test/db`
    `mkdir -p ./tmp.test/db`
    db = RecordModelMMDB.open(@klass.model, "./tmp.test/db/", 0, 100, 0, 100*1_000_000, false) 

    arr = @klass.make_array(1_000_000)
    1000_000.times do |i|
      arr << @klass.new(:d => i)
    end

    100.times do |i|
      p i
      db.put_bulk(arr)
    end

    db.close
  end

end
