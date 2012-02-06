spec = Gem::Specification.new do |s|
  s.name = 'RecordModel'
  s.version = '0.1'
  s.summary = 'RecordModel'
  s.author = 'Michael Neumann'
  s.license = 'BSD License'
  s.files = ['README', 'RecordModel.gemspec',
             'include/RecordModel.h', 'include/RM_Types.h', 'include/RM_Token.h', 
	     'include/LineReader.h', 
	     'include/FileReader.h', 'include/FdFileReader.h',
	     'include/PosixFileReader.h', 'include/GzipFileReader.h',
	     'include/XzFileReader.h', 'include/AutoFileReader.h',
             'lib/RecordModel/RecordModel.rb', 'lib/RecordModel/Query.rb',
             'lib/RecordModel/LineParser.rb',
             'ext/RecordModel/RecordModel.cc',
             'ext/RecordModel/extconf.rb']
  s.extensions = ['ext/RecordModel/extconf.rb']
  s.require_paths = ['lib']
end
