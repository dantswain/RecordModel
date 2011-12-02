$LOAD_PATH << "./ext/RecordModel"
$LOAD_PATH << "./ext/LevelDB"
$LOAD_PATH << "./src"

require "RecordModel"
require "RecordModelLevelDB"

ConversionItem = RecordModel.define do |r|
  r.key :campaign_id, :uint64
  r.key :timestamp, :uint64

  r.val :uid_upper, :uint64
  r.val :uid_lower, :uint64
  r.val :company_id, :uint64
  r.val :conversion_type, :uint64
end

db = RecordModelLevelDB.open("test.leveldb", ConversionItem)
c = ConversionItem.new

s = Time.now
10_000_000.times do |i|
  if i % 100_000 == 0
    e = Time.now
    puts "#{e-s}: #{i}"
    s = e
  end
  c.timestamp = rand(100_000_000) 
  db.put(c)
end
