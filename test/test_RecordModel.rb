require 'test/unit'

$LOAD_PATH << "../ext/RecordModel" 
$LOAD_PATH << "../lib" 
require 'RecordModel/RecordModel'

class TestRecordModel < Test::Unit::TestCase

  def setup
    @klass = RecordModel.define do |r|
      r.key :a, :uint8
      r.key :b, :uint16
      r.key :c, :uint32
      r.key :d, :uint64
      r.val :e, :double
      r.val :f, :hexstr, :size => 16
      r.key :g, :timestamp
      r.key :h, :timestamp_desc
    end
  end

  def test_model_size
    assert_equal(55, @klass.model.size)
  end

  def test_default_values
    k0 = RecordModel.define do |r|
      r.key :a, :uint8
    end
    k1 = RecordModel.define do |r|
      r.key :a, :uint8, :default => 99
    end
    assert_equal 0, k0.new.a
    assert_equal 99, k1.new.a
  end

  def test_timestamp_desc
    k0 = RecordModel.define do |r|
      r.key :ts, :timestamp
    end
    k1 = RecordModel.define do |r|
      r.key :ts, :timestamp_desc
    end

    a = k0.new(:ts => 10)
    b = k0.new(:ts => 5)
    assert_equal(1, a <=> b)

    a = k1.new(:ts => 10)
    b = k1.new(:ts => 5)
    assert_equal(-1, a <=> b)
  end

  def test_init
    rec = @klass.new
    assert_equal(0, rec.a)
    assert_equal(0, rec.b)
    assert_equal(0, rec.c)
    assert_equal(0, rec.d)
    assert_equal(0.0, rec.e)
    assert_equal("00" * 16, rec.f)
    assert_equal(0, rec.g)
    assert_equal(0, rec.h)
  end

  def test_min_max
    rec = @klass.new

    min_max(rec, 0, 0, 2**8 - 1)
    min_max(rec, 1, 0, 2**16 - 1)
    min_max(rec, 2, 0, 2**32 - 1)
    min_max(rec, 3, 0, 2**64 - 1)
    #min_max(rec, 4, -(1.0/0), (1.0/0)) # XXX: min/max for float
    min_max(rec, 5, '00' * 16, 'FF' * 16)
    min_max(rec, 6, 0, 2**64 - 1)
    min_max(rec, 7, 2**64 - 1, 0)
  end

  def min_max(rec, fld, min, max)
    rec.set_min(fld)
    assert_equal(min, rec[fld])
    rec.set_max(fld)
    assert_equal(max, rec[fld])
  end

  def test_from_string
    rec = @klass.new
    
    from_string(rec, 0, 0, "0")
    from_string(rec, 0, 255, "255")
    from_string(rec, 0, 128, "128")
    from_string(rec, 0, RuntimeError, "256")

    from_string(rec, 1, 0, "0")
    from_string(rec, 1, 2**16-1, (2**16-1).to_s)
    from_string(rec, 1, RuntimeError, (2**16).to_s)

    from_string(rec, 2, 0, "0")
    from_string(rec, 2, 2**32-1, (2**32-1).to_s)
    from_string(rec, 2, RuntimeError, (2**32).to_s)

    from_string(rec, 3, 0, "0")
    from_string(rec, 3, 2**64-1, (2**64-1).to_s)
    # XXX
    #from_string(rec, 3, ArgumentError, (2**64).to_s)

    from_string(rec, 4, 0.0, "0")
    from_string(rec, 4, 0.0, "0.0")
    from_string(rec, 4, 1.2, "1.2")
    from_string(rec, 4, 1000.0, "1000.0")
    from_string(rec, 4, -1000.0, "-1000.0")

    from_string(rec, 5, '00' * 16, "0")
    from_string(rec, 5, 'FF' * 16, "FF" * 16)
    from_string(rec, 5, '00' * 15 + 'AD', "AD")

    for i in 6 .. 7
      from_string(rec, 6, 0, "0")
      from_string(rec, 6, 0, "0.0")
      from_string(rec, 6, 0, "0.000")
      from_string(rec, 6, 100, "0.1")
      from_string(rec, 6, 123, "0.123")
      from_string(rec, 6, 123, "0.1234")
      from_string(rec, 6, 1999123, "1999.1234")
    end
  end

  def from_string(rec, fld, exp, str)
    if exp.kind_of?(Class)
      assert_raise(exp) do rec.set_from_string(fld, str) end 
    else
      rec.set_from_string(fld, str)
      assert_equal(exp, rec[fld])
    end
  end

end
