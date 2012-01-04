require 'test/unit'

$LOAD_PATH << "../ext/RecordModel" 
$LOAD_PATH << "../lib" 
require 'RecordModel'

class TestRecordModel < Test::Unit::TestCase

  def setup
    @klass = RecordModel.define do |r|
      r.key :a, :uint8
      r.key :b, :uint16
      r.key :c, :uint32
      r.key :d, :uint64
      r.val :e, :double
      r.val :f, :hexstr, 16
    end
  end

  def test_model_size
    assert_equal(39, @klass.model.size)
  end

  def test_init
    rec = @klass.new
    assert_equal(0, rec.a)
    assert_equal(0, rec.b)
    assert_equal(0, rec.c)
    assert_equal(0, rec.d)
    assert_equal(0.0, rec.e)
    assert_equal("00" * 16, rec.f)
  end

  def test_min_max
    rec = @klass.new

    min_max(rec, 0, 0, 2**8 - 1)
    min_max(rec, 1, 0, 2**16 - 1)
    min_max(rec, 2, 0, 2**32 - 1)
    min_max(rec, 3, 0, 2**64 - 1)
    #min_max(rec, 4, -(1.0/0), (1.0/0)) # XXX: min/max for float
    min_max(rec, 5, '00' * 16, 'FF' * 16)
  end

  def min_max(rec, fld, min, max)
    rec.set_min(fld)
    assert_equal(min, rec[fld])
    rec.set_max(fld)
    assert_equal(max, rec[fld])
  end

end
