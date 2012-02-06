spec = Gem::Specification.new do |s|
  s.name = 'RecordModelMMDB'
  s.version = '0.1'
  s.summary = 'RecordModel Main memory driver'
  s.author = 'Michael Neumann'
  s.license = 'BSD License'
  s.files = ['README', 'RecordModelMMDB.gemspec',
             'include/RecordModel.h', 'include/RM_Types.h', 'include/RM_Token.h', 'include/LineReader.h',
             'lib/MMDB/DB.rb', 'lib/MMDB/DBMS.rb',
             'lib/MMDB/CommitLog.rb',
             'ext/MMDB/MMDB.cc', 'ext/MMDB/MmapFile.h',
             'ext/MMDB/extconf.rb']
  s.extensions = ['ext/MMDB/extconf.rb']
  s.require_paths = ['lib']
end
