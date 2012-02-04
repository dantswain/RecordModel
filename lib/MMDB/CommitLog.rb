module MMDB; end

class MMDB::CommitLog
  BLKSIZE = 512

  def initialize(filename)
    @filename = filename
  end

  def all(n=nil)
    arr =
    if File.exist?(@filename)
      sz = File.size(@filename)
      raise "Invalid file size" if sz % BLKSIZE != 0
      n = sz / BLKSIZE unless n 
      if sz == 0
        []
      else
	File.open(@filename, "r") {|f|
          (0 ... n).map { f.read(BLKSIZE) }
        }
      end
    else
      return []
    end
    raise if arr.size != n
    raise if arr.any? {|i| i.size != BLKSIZE}
    return arr
  end

  def last
    if File.exist?(@filename)
      sz = File.size(@filename)
      raise "Invalid file size" if sz % BLKSIZE != 0
      if sz == 0
        nil
      else
        File.read(@filename, BLKSIZE, sz - BLKSIZE)
      end
    else
      nil
    end
  end

  def truncate(n)
    if File.exist?(@filename)
      File.truncate(@filename, BLKSIZE*n)
    end
  end

  def append(str, filler=" ")
    raise ArgumentError if str.size > BLKSIZE
    str = str.ljust(BLKSIZE, filler)
    raise if str.size != BLKSIZE
    raise if str.bytesize != BLKSIZE

    File.open(@filename, "a+") {|f|
      f.write(str)
      f.fsync
    }
  end
end
