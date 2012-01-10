require 'test/unit'

$LOAD_PATH << "../ext/RecordModel" 
$LOAD_PATH << "../ext/MMDB" 
$LOAD_PATH << "../lib" 
require 'RecordModel/RecordModel'
require 'MMDB/MMDB'

class TestMMDB < Test::Unit::TestCase

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
    db = MMDB.open(@klass.model, "./tmp.test/db/", 0, 0, 0, 0, false) 
    db.close
    `rm -rf ./tmp.test/db`
  end

  def test_write_and_query
    `rm -rf ./tmp.test/db`
    `mkdir -p ./tmp.test/db`
    db = MMDB.open(@klass.model, "./tmp.test/db/", 0, 1, 0, 100_000, false) 

    arr = @klass.make_array(100_000)
    100_000.times do |i|
      arr << @klass.new(:a => i % 2, :d => i)
    end

    db.put_bulk(arr)

    assert_equal 6, db.query(@klass, :d => 5 .. 10).count
    assert_equal 3, db.query(@klass, :a => 0, :d => 5 .. 10).count
    assert_equal 3, db.query(@klass, :a => 1, :d => 5 .. 10).count
    assert_equal 0, db.query(@klass, :a => 2, :d => 5 .. 10).count

    db.close
  end

end
