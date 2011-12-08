$LOAD_PATH << "../ext/RecordModel"
$LOAD_PATH << "../ext/LevelDB"
#$LOAD_PATH << "./ext/KyotoCabinet"
#$LOAD_PATH << "./ext/TokyoCabinet"
$LOAD_PATH << "../lib"

require "RecordModel"
require "RecordModelLevelDB"
#require "RecordModelKCDB"
#require "RecordModelTCDB"

ConversionItem = RecordModel.define do |r|
  r.key :uid_0, :uint64
  r.key :uid_1, :uint64
  #r.acc :uid, :uint64_x2, :uid_0, :uid_1

  r.key :timestamp, :uint64

  r.val :a, :uint64
  r.val :b, :uint64
  r.val :c, :uint64
  r.val :d, :uint64
  r.val :e, :uint64
  r.val :f, :uint64
  r.val :g, :uint8
  r.val :h, :uint8
end

def insert(db, n, x=100_000_000)
  c = ConversionItem.new
  n.times do
    c.uid_0 = rand(x)
    c.uid_1 = rand(x)
    c.timestamp = rand(x)
    db.add(c) or p("not inserted")
  end
end

dbs = []
ts = []

Integer(ARGV[0] || 4).times do |i| 
  db = RecordModelLevelDB.open("par_#{i}.leveldb", ConversionItem)
  t = Thread.new { insert(db, 1_000_000) }
  dbs << db
  ts << t
end 

ts.each {|t| t.join}
dbs.each {|db| db.close}
