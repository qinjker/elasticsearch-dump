# elasticsearch-dump
A tool for elasticsearch index dumping &amp; restoring.

## install
```
bundle install
```

## dump index data to a file
```
esdump.rb -i http://localhost:9200/my_index -o my_index.data -t data
```

## dump index mapping to a file
``` 
esdump.rb -i http://localhost:9200/my_index -o my_index_mapping.data -t mapping
```
