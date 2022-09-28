const path = require("path");
const storage = require("../storage");
const { debug } = console;
const prefixesSeen = [];
const COMPACT_CHUNK_SIZE = process.env.COMPACT_CHUNK_SIZE
  ? parseFloat(process.env.COMPACT_CHUNK_SIZE)
  : 50_000_000;

async function saveChunk (prefix, chunk) {
  if (Object.keys(chunk).length < 1) return;
  const ts = new Date().toISOString();
  
  // first, empty out original chunk 
  // because we'll async send to aws, so it could still be modified
  const chunkCopy = {...chunk}
  Object.keys(chunk).map(k => delete chunk[k]);

  debug(
    "saving chunk, keys:",
    Object.keys(chunkCopy).length,
    "size:",
    JSON.stringify(chunkCopy).length,
    "=>",
    prefix,
    ts
  );
  const key = path.join("compacted", prefix, ts);
  
  await storage.put(`${key}.keys.txt`, Object.keys(chunkCopy).join("\n"), false);
  await storage.put(`${key}.data.json`, chunkCopy);
  // delete the uncompacted keys after we're sure that they're safe
  await Promise.all(Object.keys(chunkCopy).map((k) => storage.del(k)));
}

async function compactFolder(prefix = "") {
  if (!prefix) return;
  if (prefixesSeen.includes(prefix)) return;
  prefixesSeen.push(prefix);
  
  let promises = [];
  let subFolders = []
  let chunkBuffer = {};
  let chunkSize = 0;
  for await (const k of storage.list(prefix)) {
    if (k.startsWith("compacted/")) continue;
    // don't compress folders like auth and zips
    if (path.basename(k) == "__auth") continue;
    if (path.basename(k) == '__weeklyzips') continue;

    if (k.endsWith("/")) {
      subFolders.push(k)
    } else {
      let p = Promise.resolve().then(async () => {
        debug("processing key:", k);
        const f = await storage.get(k);
        // skip non-json data
        try {
          chunkBuffer[k] = JSON.parse(f);
          chunkSize += f.length;
        } catch (e) {
          debug("skipping non json file:", k);
          return;
        }
        // debug('chunkSize:', chunkSize);
        if (chunkSize > COMPACT_CHUNK_SIZE) {
          // clear out values and send to archive
          chunkSize = 0;
          let chunkCopy = {...chunkBuffer}
          chunkBuffer = {}
          await saveChunk(prefix, chunkCopy);
        }
      });
      promises.push(p);
      // don't make too many promises
      if (promises.length % 100 === 0) 
        await Promise.all(promises);
    }
  }
  await Promise.all(promises);
  await saveChunk(prefix, chunkBuffer);
  await Promise.all(subFolders.map(compactFolder))
}

module.exports = compactFolder;
