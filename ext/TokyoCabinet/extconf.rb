require 'mkmf'
raise unless have_library "tokyocabinet" 
create_makefile('RecordModelTCDBExt') 
