package help

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Command displays help for command-line sub-commands.
type Command struct {
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(usage))
	return nil
}

//TODO zhexuany need finish all these commands. For now, we only provide very basic commands such as join, add-meta, add-date
const usage = `
Configure and start an cluster InfluxDB .

Usage: influxd-ctl [options] <command> [options] [<args>]

The commands are:

add-data            Add a data node
add-meta            Add a meta node
backup              Backup a cluster
copy-shard          Copy a shard between data nodes
copy-shard-status   Show all active copy shard tasks
join                Join a meta or data node
kill-copy-shard     Abort an in-progress shard copy
leave               Remove a meta node
remove-data         Remove a data node
remove-meta         Remove a meta node
remove-shard        Remove a shard from a data node
restore             Restore a backup of a cluster
show                Show cluster members
show-shards         Shows the shards in a cluster
update-data         Update a data node
token               Generates a signed JWT token
truncate-shards     Truncate current shards

The options are:

-auth-type string
    Type of authentication to use (none, basic, jwt) (default "none")
-bind string
    Bind HTTP address of a meta node (default "localhost:8091")
-bind-tls
    Use TLS
-config string
    Config file path
-k	Skip certficate verification (ignored without -bind-tls)
-pwd string
    Password (ignored without -auth-type jwt)
-secret string
    JWT shared secret (ignored without -auth-type jwt)
-user string
    User name (ignored without -auth-type basic | jwt)-auth-type string
    Type of authentication to use (none, basic, jwt) (default "none")
-bind string
    Bind HTTP address of a meta node (default "localhost:8091")
-bind-tls
    Use TLS
-config string
    Config file path
-k	Skip certficate verification (ignored without -bind-tls)
-pwd string
    Password (ignored without -auth-type jwt)
-secret string
    JWT shared secret (ignored without -auth-type jwt)
-user string
    User name (ignored without -auth-type basic | jwt)
`
