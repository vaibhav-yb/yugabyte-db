## yba auth

Authenticate yba cli

### Synopsis

Authenticate the yba cli through this command by providing the host and API Key.

```
yba auth [flags]
```

### Options

```
  -h, --help   help for auth
```

### Options inherited from parent commands

```
  -a, --apiKey string      YugabyteDB Anywhere api key
      --config string      config file, default is $HOME/.yba-cli.yaml
      --debug              use debug mode, same as --logLevel debug
  -H, --host string        YugabyteDB Anywhere Host, default to http://localhost:9000
  -l, --logLevel string    select the desired log level format, default to info
      --no-color           disable colors in output , default to false
  -o, --output string      select the desired output format (table, json, pretty), default to table
      --timeout duration   wait command timeout,example: 5m, 1h. (default 168h0m0s)
      --wait               wait until the task is completed, otherwise it will exit immediately, default to true
```

### SEE ALSO

* [yba](yba.md)	 - yba - Command line tools to manage your YugabyteDB Anywhere (Self-managed Database-as-a-Service) resources.

