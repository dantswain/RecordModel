require 'test/unit'

$LOAD_PATH << "../ext/RecordModel" 
$LOAD_PATH << "../lib" 
require 'RecordModel/AutoFileReader'

class TestRecordModel < Test::Unit::TestCase

  def setup
    `echo -n "hallo test" | xz > test.xz`
  end

  def teardown
    `rm test.xz`
  end

  def test_read
    AutoFileReader.open('test.xz') {|io|
      assert_equal 'h', io.read(1)
      assert_equal "allo test", io.read(1000)
      assert_equal nil, io.read(100)
    }
  end


  def test_single
    AutoFileReader.open('test.xz') {|io|
      str = ""
      while c = io.read(1)
        str << c
      end
      assert_equal "hallo test", str
    }
  end

end
