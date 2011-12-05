spec = Gem::Specification.new do |s|
  s.name = 'RecordModelLevelDB'
  s.version = '0.1'
  s.summary = 'RecordModel LevelDB driver'
  s.author = 'Michael Neumann'
  s.license = 'BSD License'
  s.files = ['README', 'RecordModelLevelDB.gemspec', 'include/RecordModel.h',
             'lib/RecordModelLevelDB.rb', 'ext/LevelDB/LevelDB.cc',
             'ext/LevelDB/extconf.rb']
  s.extensions = ['ext/LevelDB/extconf.rb']
  s.require_paths = ['lib']
end
