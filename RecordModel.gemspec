spec = Gem::Specification.new do |s|
  s.name = 'RecordModel'
  s.version = '0.1'
  s.summary = 'RecordModel'
  s.author = 'Michael Neumann'
  s.license = 'BSD License'
  s.files = ['README', 'RecordModel.gemspec', 'include/RecordModel.h',
             'lib/RecordModel.rb', 'ext/RecordModel/RecordModel.cc',
             'ext/RecordModel/extconf.rb']
  s.extensions = ['ext/RecordModel/extconf.rb']
  s.require_paths = ['lib']
end
