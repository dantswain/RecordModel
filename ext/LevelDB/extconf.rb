require 'mkmf'
raise unless have_library "leveldb"
create_makefile('RecordModelLevelDBExt') 
