MemcLoad Go
======
an app for honing the skill of converting an app into a `Python` to `Golang`

Run
---
* install golanng 
* run build 
* run application

Options
------

| Options | Default | Description |
| ------- | ----- |-------------|
`timeout`|3|timeout in seconds for all calls to a server memcached. Defaults to 3 seconds.|
`retry`|3|retry connection to set value to memcached. Defaults to 3 attempts
`dry`| False| 
`pattern`| "/data/appsinstalled/*.tsv.gz"| 
`idfa`| "127.0.0.1:33013"| 
`gaid`| "127.0.0.1:33014"| 
`adid`| "127.0.0.1:33015"| 
`dvid`| "127.0.0.1:33016"| 

For test
-------------
```shell script
go run memcloaderGo --dry  --pattern sample.zip
```
