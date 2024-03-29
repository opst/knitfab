knit cli
==========

## Outline of knit cli

Command line tool `knit` is built from several subcommands.

```
knit <subcommand> [<sub-sub-command> ...] [...flags] [...args]
```

All subcommands accept common flags.

- `--profile` : name of knit profile.
    - default: content of the file named `.knitprofile` in current or most recent ancestral directory.
- `--profile-store` : name of knit profile. default: `~/.knit/profile`
- `--env` : user's project envirionment. default: `./.knitenv`

"knit profile" holds settings that declare "where knit api is to be used"(= url) and "how do connect to knit api"(= authentication & custom ca certs).

They are stored in the profile store, and looked up by the name.
CLI commands may requests to the endpoint determined by the profile to invoke Knitfab Web API

## How to implement (sub)command

Subcommands are in `./subcommands` package.
For example, `knit data push` is in `./subcommands/data/push/command.go`.

Each subcommands should be a `cmd/knit/commandline/command.KnitCommand[T]`.

Your subcommand is instantiated in `main.go` .

For each subcommand should implement 4 methods:

- `Name` : the name of command.
- `Usage` : definition of flags and arguments.
- `Help` : help messages.
- `Execute` : entrypoint of the subcommand.

### `Usage`

`Usage` should returns `cmd/knit/commandline/usage.Usage[T]`.
Type parameter `T` determines the type parameter of `KnitCommand` .

`usage.Usage[T]` describes flags and arguments of the command and comes down to `flag.FlagSet`.

```go
func (*Command) Usage() usage.Usage[Flags] {
    return usage.New(
        Flags{ ... },
        usage.Args{
            ...
        },
    )
}
```

The first parameter, `Flags` in above, tells **flags and default values**.

`usage.Usage` bonds fieds of `Flags` to commandline flags, and uses field values as default value for each flags.

Type for flag should be struct and its filed can have `flag` tag.

```go
type Flags struct {
    Flag1 string `flag:"flag-1"`
    Flag2 int    `flag:"another-flag,short=f,metavar=SOMETHING,help=help message"`
}
```

`flag` tag says "this field is for flag". Field types should be type of `flag.FlagSet.XXXVar` or `flag.Value`.

For example, by the definition in above, we have flags like `COMMAND --flag-1 ... --another-flag ...`.

The first element in tag `flag` represents the name of flag.

There are optional elements.
For more details, see `cmd/knit/commandline/flag/flagger.New` (`usage.Usage` uses this).

The second parameter `usage.Args` determines positional arguments.

Each elements in `usage.Args` is in form below:

```go
usage.Args{
    {
        Name: "name", // argument name in help message
        Required: true, // this is requried argument (= true) or not (= false).
        Repeatable: true, // this argument can be repeatable (= true) or not (= false).
        Help: "help message...",
    },
    // ...
}
```

If `Repeatable: true`, the argument consumes values eagarly.

For example, let us have `usage.Args` like below:

```go
usage.Args{
    {Name: "arg1", Required: true},
    {Name: "arg2", Repeated: true},  // repeated!
    {Name: "arg3", Required: true},
    {Name: "arg4", Repeated: true},  // repeated!
}
```

Then, the command is invoked as `COMMAND a b c d e`.
This will assign values to arguments as:

- `arg1`: `[a]`
- `arg2`: `[b, c, d]`
- `arg3`: `[e]`
- `arg4`: `[]`

`arg2` takes values as many as possible.

### `Execute`

`Execute` is the entrypoint of knitfab command.

It takes arguments as below:

- `context.Context`
- `*log.Logger`
- `KnitEnv`
- `KnitClient`
- `usage.FlagSet[T]`

Use `*log.Logger` to print logs, hints or other messages for user.
Note that outputs of command should be written to `os.Stdout`, not to logger.

`KnitEnv` holds `.knitenv`'s content.

`KnitClient` are rest client for Knitfab Web API.
This is configured with knit profile, before passed to `Execute`.

`usage.FlagSet[T]` is *parsed* flags and arguments.

`usage.FlagSet[T]` has 2 properties: `Flags T` and `Args map[string][]string`.
`usage.FlagSet.Flags` is `T`, having values from commandline flags.
`usage.FlagSet.Args` is mapping from name of argument to values assigned to the name.

For example, let we have `Usage()` as below:

```go
type Flags struct {
    Field1 string `flag:"flag1"`
    Field2 int `flag:"flag2"`
}

func (*Command) Usage() usage.Usage[Flags] {
    return usage.New(
        Flags{
            Field1: "some default value",
            Field2: 42,
        },
        usage.Args{
            {Name: "arg1", Required: false},
            {Name: "arg2", Required: true},
        },
    )
}
```

And, invoking command as `COMMAND --flag1 bar --flag2 1000 abc def` comes to calling `Execute` as

```go
cmd.Execute(
    ...,
    usage.FlagSet[Flag]{
        Flags: Flags{
            Field1: "bar",
            Field2: 1000,
        },
        map[string][]string{
            "arg1": {"abc"},
            "arg2": {"def"},
        }
    }
)
```

Another example: `COMMAND --flag1 foo abc` comes to

```go
cmd.Execute(
    ...,
    usage.FlagSet[Flag]{
        Flags: Flags{
            Field1: "bar",
            Field2: 42,  // default value is used
        },
        map[string][]string{
            "arg1": {},      // yield to required argument.
            "arg2": {"abc"},
        }
    }
)
```

`Execute` returns `error`. Retuning `nil`, command will success.

Return `cmd/knit/commandline/command.ErrUsage` to exit with usage error (`2`) and show the error & help messages.
If it returns other error, it exits with error (`1`) and shows error message.

### `Help`

`Help()` provides help message.

You can use placeholder `{{ .Command }}` in message to be filled with "full" command name from roo command `knit` to sub(-sub(-sub...))command.

## register subcommand

To register subcommand to the parent, build `subcommand.Command` and pass to the parent.

`cmd/knit/commadline/command.Build` builds `command.KnitCommand` to `subcommand.Command`.

See `cmd/knit/subcommands/data/command.go`, for example.
