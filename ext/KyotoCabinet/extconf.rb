require 'mkmf'
raise unless have_library "kyotocabinet" 
create_makefile('RecordModelKCDBExt') 
