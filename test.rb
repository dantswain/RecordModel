$LOAD_PATH << "./ext/RecordModel"
$LOAD_PATH << "./ext/LevelDB"
$LOAD_PATH << "./ext/KyotoCabinet"
$LOAD_PATH << "./ext/TokyoCabinet"
$LOAD_PATH << "./src"

require "RecordModel"
require "RecordModelLevelDB"
require "RecordModelKCDB"
require "RecordModelTCDB"

ConversionItem = RecordModel.define do |r|
  r.key :campaign_id, :uint64
  r.key :timestamp, :uint64

  r.val :v1, :double
  r.val :v2, :uint32
end

c = ConversionItem.new

N = ARGV[0].to_i

dbname = ARGV[2] || 'test'

db = case ARGV[1]
when 'leveldb'
  RecordModelLevelDB.open("#{dbname}.leveldb", ConversionItem)
when 'kyotocabinet'
  RecordModelKCDB.open("#{dbname}.kcdb", ConversionItem)
when 'tokyocabinet'
  RecordModelTCDB.open("#{dbname}.tcdb", ConversionItem)
else
  raise
end

nh = N/100

s = Time.now
N.times do |i|
  if i % nh == 0
    e = Time.now
    puts "#{e-s}: #{i}"
    s = e
  end
  c.timestamp = rand(100_000_000) 
  db.put(c)
end
