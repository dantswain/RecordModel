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
      r.key :s, :string, :size => 32
    end
  end

  def test_parse_line1
    item = @klass.new
    res = item.parse_line("  22   2.3 10000.999  ",
      [:a, :e, :g].map {|fld| item.sym_to_fld_idx(fld)}, " ")
    assert_equal 3, res
    assert_equal 22, item.a
    assert_equal 2.3, item.e
    assert_equal 10000_999, item.g
  end

  def test_parse_line2
    item = @klass.new
    res = item.parse_line("22,2.3,10000.999",
      [:a, :e, :g].map {|fld| item.sym_to_fld_idx(fld)}, ",")
    assert_equal 3, res
    assert_equal 22, item.a
    assert_equal 2.3, item.e
    assert_equal 10000_999, item.g
  end

  def test_model_size
    assert_equal(87, @klass.model.size)
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
    assert_equal("\000" * 32, rec.s)
  end

  def test_min_max
    rec = @klass.new

    min_max(rec, :a, 0, 2**8 - 1)
    min_max(rec, :b, 0, 2**16 - 1)
    min_max(rec, :c, 0, 2**32 - 1)
    min_max(rec, :d, 0, 2**64 - 1)
    #min_max(rec, :e, -(1.0/0), (1.0/0)) # XXX: min/max for float
    min_max(rec, :f, '00' * 16, 'FF' * 16)
    min_max(rec, :g, 0, 2**64 - 1)
    min_max(rec, :h, 2**64 - 1, 0)
    min_max(rec, :s, "\x00" * 32, "\xFF" * 32)
  end

  def min_max(rec, fld, min, max)
    idx = rec.sym_to_fld_idx(fld)
    rec.set_min(idx)
    assert_equal(min, rec[idx])
    rec.set_max(idx)
    assert_equal(max, rec[idx])
  end

  def test_from_string
    rec = @klass.new
    
    from_string(rec, :a, 0, "0")
    from_string(rec, :a, 255, "255")
    from_string(rec, :a, 128, "128")
    from_string(rec, :a, RuntimeError, "256")

    from_string(rec, :b, 0, "0")
    from_string(rec, :b, 2**16-1, (2**16-1).to_s)
    from_string(rec, :b, RuntimeError, (2**16).to_s)

    from_string(rec, :c, 0, "0")
    from_string(rec, :c, 2**32-1, (2**32-1).to_s)
    from_string(rec, :c, RuntimeError, (2**32).to_s)

    from_string(rec, :d, 0, "0")
    from_string(rec, :d, 2**64-1, (2**64-1).to_s)
    # XXX
    #from_string(rec, :d, ArgumentError, (2**64).to_s)

    from_string(rec, :e, 0.0, "0")
    from_string(rec, :e, 0.0, "0.0")
    from_string(rec, :e, 1.2, "1.2")
    from_string(rec, :e, 1000.0, "1000.0")
    from_string(rec, :e, -1000.0, "-1000.0")

    from_string(rec, :f, '00' * 16, "0")
    from_string(rec, :f, 'FF' * 16, "FF" * 16)
    from_string(rec, :f, '00' * 15 + 'AD', "AD")

    for i in [:g, :h]
      from_string(rec, i, 0, "0")
      from_string(rec, i, 0, "0.0")
      from_string(rec, i, 0, "0.000")
      from_string(rec, i, 100, "0.1")
      from_string(rec, i, 123, "0.123")
      from_string(rec, i, 123, "0.1234")
      from_string(rec, i, 1999123, "1999.1234")
    end

    from_string(rec, :s, 'abcdefgh' + "\000"*(32-8), 'abcdefgh') 
    from_string(rec, :s, 'a' + "\000"*31, 'a') 
  end

  def from_string(rec, fld, exp, str)
    idx = rec.sym_to_fld_idx(fld)
    if exp.kind_of?(Class)
      assert_raise(exp) do rec.set_from_string(idx, str) end 
    else
      rec.set_from_string(idx, str)
      assert_equal(exp, rec[idx])
    end
  end

end
