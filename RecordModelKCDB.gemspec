spec = Gem::Specification.new do |s|
  s.name = 'RecordModelKCDB'
  s.version = '0.1'
  s.summary = 'RecordModel KyotoCabinet driver'
  s.author = 'Michael Neumann'
  s.license = 'BSD License'
  s.files = ['README', 'RecordModelKCDB.gemspec', 'include/RecordModel.h',
             'lib/RecordModelKCDB.rb', 'ext/KyotoCabinet/KyotoCabinet.cc',
             'ext/KyotoCabinet/extconf.rb']
  s.extensions = ['ext/KyotoCabinet/extconf.rb']
  s.require_paths = ['lib']
end
