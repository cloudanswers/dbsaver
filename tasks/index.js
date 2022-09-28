const cliProgress = require("cli-progress");
const Bottleneck = require("bottleneck/es5");
const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const { basename } = require("path");
const settings = require("../settings");
const storage = require("../storage");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

const limiter = new Bottleneck({
  maxConcurrent: 5,
});

async function copyToBackblaze(prefix = "") {
  debug("starting copy to backblaze...");
  const bar1 = new cliProgress.SingleBar({});
  bar1.start(1000);
  let promiseQueue = [];
  for await (const k of storage.list(prefix)) {
    debug({ k });
    if (k.endsWith("/")) {
      await copyToBackblaze(k);
    } else {
      debug("processing", k);
      // await here to slow it down
      let p = limiter
        .schedule(() => {
          return storage.exists(k, storage.bb).then(async (exist) => {
            // FIXME check size or hash
            if (!exist) {
              debug("copying file", k);
              const f = await storage.get(k);
              return storage.put(k, f, false, storage.bb);
            }
          });
        })
        .then(() => bar1.increment());
      promiseQueue.push(p);
    }
    if (promiseQueue.length > 100) {
      await Promise.all(promiseQueue);
      promiseQueue = [];
    }
  }
}

yargs(hideBin(process.argv))
  .command(
    "compact [folder]",
    "compact a folder",
    (yargs) => {
      return yargs.positional("folder", {
        describe: "folder to compact",
        required: true,
      });
    },
    (argv) => {
      if (argv.folder) require("./compactFolder")(argv.folder);
    }
  )
  .command(
    "spreadsheetExport [connectionPrefix] [objectFilter]",
    "export a full spreadsheet for data migration",
    (yargs) => {
      return yargs
        .positional("connectionPrefix", {
          describe: "connection prefix",
        })
        .positional("objectFilter", {
          describe: "objectFilter",
        });
    },
    (argv) => {
      const spreadsheetExport = require("./spreadsheetExport");
      if (argv.connectionPrefix)
        spreadsheetExport(argv.connectionPrefix, argv.objectFilter);
    }
  )
  .command(
    "replicate [sourceConnectionPrefix] [destConnectionPrefix] [objectNameFilter]",
    "export a full spreadsheet for data migration",
    (yargs) => {
      return yargs.positional("connectionPrefix", {
        describe: "connection prefix",
      });
    },
    (argv) => {
      if (!argv.sourceConnectionPrefix)
        return console.log("missing sourceConnectionPrefix");
      if (!argv.destConnectionPrefix)
        return console.log("missing destConnectionPrefix");
      require("./replicate")(
        argv.sourceConnectionPrefix,
        argv.destConnectionPrefix,
        argv.objectNameFilter
      );
    }
  )
  .command(
    "filesExport [connectionPrefix] [objectNameFilter]",
    "export all files and attachments",
    (yargs) => {
      return yargs
        .positional("connectionPrefix", {
          describe: "connection prefix",
        })
        .positional("objectNameFilter", {
          describe: "object Name Filter",
        });
    },
    (argv) => {
      if (!argv.connectionPrefix)
        return console.error("missing connectionPrefix");
      require("./filesExport")(argv.connectionPrefix, argv.objectNameFilter);
    }
  )
  .command(
    "copyToBackblaze [prefix]",
    "copy data to backblaze",
    (yargs) => {
      return yargs.positional("prefix", {
        describe: "prefix",
      });
    },
    (argv) => {
      if (argv.prefix) copyToBackblaze(argv.prefix);
    }
  )
  .command(
    "fileLoad [conn]",
    "copy files from one org to another",
    (yargs) => {
      return yargs.positional("conn", {
        describe: "conn",
      });
    },
    (argv) => {
      if (!argv.conn) return console.error("missing conn");
      require("./fileLoad")(argv.conn);
    }
  )
  .command(
    "metadataBackup [conn]",
    "backup metadta",
    (yargs) => {
      return yargs.positional("conn", {
        describe: "conn",
      });
    },
    (argv) => {
      if (!argv.conn) return console.error("missing conn");
      require("./metadataBackup")(argv.conn).catch(console.error);
    }
  )
  .command(
    "mergeDuplicateFiles [conn]",
    "mergeDuplicateFiles",
    (yargs) => {
      return yargs.positional("conn", {
        describe: "conn",
      });
    },
    (argv) => {
      if (!argv.conn) return console.error("missing conn");
      require("./mergeDuplicateFiles")(argv.conn).catch(console.error);
    }
  )
  .command(
    "truncateTable [conn] [tableName]",
    "truncateTable",
    (yargs) => {
      return yargs
        .positional("conn", {
          describe: "conn",
        })
        .positional("tableName", {
          describe: "tableName",
        });
    },
    (argv) => {
      if (!argv.conn) return console.error("missing conn");
      if (!argv.tableName) return console.error("missing tableName");
      require("./truncateTable")(argv.conn, argv.tableName).catch(
        console.error
      );
    }
  )
  .command(
    "spreadsheetCompare [conn1] [conn2]",
    "spreadsheetCompare",
    (yargs) => {
      return yargs
        .positional("conn1", {
          describe: "conn1",
        })
        .positional("conn2", {
          describe: "conn2",
        })
        .positional("filter", {
          describe: "filter",
        });
    },
    (argv) => {
      if (!argv.conn1) return console.error("missing conn1");
      if (!argv.conn2) return console.error("missing conn2");
      require("./spreadsheetCompare")(
        argv.conn1,
        argv.conn2,
        argv.filter
      ).catch(console.error);
    }
  )
  .command(
    "saveToS3 [conn1]",
    "saveToS3",
    (yargs) => {
      return yargs.positional("conn1", {
        describe: "conn1",
      });
    },
    (argv) => {
      if (!argv.conn1) return console.error("missing conn1");
      require("./saveToS3")(argv.conn1).catch(console.error);
    }
  )

  .parse();
