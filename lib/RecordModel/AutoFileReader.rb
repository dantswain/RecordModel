require 'RecordModelExt'

class AutoFileReader
  def self.open(path, buflen=2**16, &block)
    obj = _open(path, buflen)
    if block
      begin
        block.call(obj)
      ensure
        obj.close
      end
      return nil
    else
      return obj
    end
  end
end
