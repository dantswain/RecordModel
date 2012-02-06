require 'mkmf'

have_library('z') || raise
have_library('lzma') || raise
create_makefile('RecordModelExt')
