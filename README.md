# twitch-messages

A tool to subscribe to Twitch channels and store them efficiently on disk

## Build the Tools

You can start by building the binaries that will be used in the next steps:

```bash
cargo build --release --bins
```

And copy the binaries for convenience:

```bash
cp target/release/writer target/release/reader .
```

## Usage

Once the binaries are at the root of the working directory you can run the `writer`.
It is a tool that creates a `database.tm` folder and will collect the Twitch messages of the channel
you want to follow.

```bash
./writer mistermv ponce mynthos
```

### Show the Segments

The database is now being fed by the `writer`, you can use the `reader` binary combined with the
`watch` command to show the progress of indexation.

```bash
watch -d ./reader print-all-segments
```

The `print-all-segments` subcommand of the `reader` displays the segments that contains batches of
messages, which are internally ordered by date.

The `-d` option on `watch` displays a diff of the changes that happen to the segments,
like compaction and newly appended segments.

### Show the Messages

There also is a simple command that shows all of the messages collected so far. The messages should
be ordered by date and time, but as the messages are ordered inside of each segments there can be
some overlaps can happen. Segments are simply read in order.

```bash
watch -d ./reader print-all-messages
```

The output of the messages is a CSV with the timestamp, channel, login and text of the message:

```
timestamp,channel,login,text
1635605018,etoiles,wezio95,ça marche SeemsGood
1635605018,fantabobshow,aickeron,VOTRE BARRE MONSIEUR
1635605018,fantabobshow,fg_rajahdakirin,C'EST TRUQUE
1635605018,inoxtag,juliegeek,Mon amoureuse il a dit ouhhhh
1635605018,inoxtag,lebossdu509,GlitchCat GlitchCat GlitchCat GlitchCat GlitchCat GlitchCat GlitchCat GlitchCat GlitchCat
1635605018,inoxtag,noscareddie_,calvitie
1635605018,inoxtag,wmarenoob,la calvitie de SARD LUL
1635605018,locklear,diobloz,KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW KEKW F
1635605018,maghla,kagishinseken_art,@okallylily  sa va?
1635605018,michou_twitch,claratison,oui
```
