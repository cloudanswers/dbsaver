const Bottleneck = require("bottleneck/es5");
const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const salesforce = require("../salesforce");
const storage = require("../storage");
const fetch = require("node-fetch");

const USE_CACHE = !!process.env.USE_CACHE;

const limiter = new Bottleneck({
  maxConcurrent: 1,
});

debug("Loading cache...");
let cache = {};
const CACHE_FOLDER = "./cache/";
if (USE_CACHE && fs.existsSync(CACHE_FOLDER)) {
  fs.readdirSync(CACHE_FOLDER).map((f) => {
    try {
      cache = {
        ...cache,
        ...JSON.parse(fs.readFileSync(CACHE_FOLDER + f, "utf-8")),
      };
    } catch (e) {
      // ignore corrupted (partially written) cache files
    }
  });
} else {
  fs.mkdirSync(CACHE_FOLDER, { recursive: true });
}
debug("cache loaded.");

function chunk(listToChunk, chunkSize) {
  let res = [[]];
  listToChunk.map((x) => {
    if (res[res.length - 1].length >= chunkSize) res.push([]);
    res[res.length - 1].push(x);
  });
  return res;
}

async function getCached(key, promiseToResolve) {
  if (USE_CACHE && cache[key]) return cache[key];
  let val = await promiseToResolve();
  if (USE_CACHE) {
    cache[key] = val;
    let cacheFile = path.join(CACHE_FOLDER, new Date().getTime() + ".json");
    fs.writeFile(cacheFile, JSON.stringify({ [key]: val }), () => {});
  }
  return val;
}

function getCacheKey(conn, ...key) {
  const orgId = conn?.accessToken.split("!")[0];
  if (!orgId) throw new Error("missing accessToken");
  return [orgId, ...key].join("/");
}

async function query(conn, soql) {
  const cacheKey = getCacheKey(conn, soql);
  return getCached(cacheKey, () => conn.query(soql));
}

async function getConnection(connectionPrefix) {
  let res;
  for await (const authKey of storage.list(
    path.join(connectionPrefix, "__auth/")
  )) {
    res = authKey;
  }
  return salesforce.getConnectionFromStorage(res);
}

async function mergeDuplicateFiles(connectionPrefix) {
  debug("processing ", connectionPrefix);
  const conn = await getConnection(connectionPrefix);
  //   let soql = `
  //     select Checksum, count(Id)
  //     from ContentVersion
  //     group by Checksum
  //     having count(Id) > 1
  //   `;
  // FIXME swap parameterize query for re-use
  let soql = `
    select Title, FileType, ContentSize, LatestPublishedVersion.Checksum, count(Id)
    from ContentDocument 
    where CreatedBy.Name = 'Some User'
    and CreatedDate > 2022-01-01T00:00:00.000Z
    group by Title, FileType, ContentSize, LatestPublishedVersion.Checksum 
    having count(Id) > 1
    `;
  let cd_soql = await conn.sobject("ContentDocument").find().toSOQL();
  let cv_soql = await conn.sobject("ContentVersion").find().toSOQL();
  let cdl_soql = await conn.sobject("ContentDocumentLink").find().toSOQL();
  cd_soql = cd_soql.replace("SELECT Id,", `SELECT Id, (${cv_soql}s),`);
  cd_soql = cd_soql.replace("SELECT Id,", `SELECT Id, (${cdl_soql}s),`);
  cd_soql = cd_soql.replace(
    "LinkedEntityId,",
    "LinkedEntityId, LinkedEntity.Name,"
  );
  cd_soql = cd_soql.replace("VersionData,", ""); // can't query this directly

  debug("Loading records...");
  let res = await query(conn, soql);
  //   debug({ res });
  for (const r of res.records) {
    debug({ r });
    let res2 = await query(
      conn,
      `${cd_soql} where LatestPublishedVersionId in (select Id from ContentVersion where Checksum = '${r.Checksum}' and ContentSize = ${r.ContentSize}) and CreatedBy.Name = 'Ida Huang' order by CreatedDate asc `
    );
    debug({ res2, records: res2.records });

    // copy links from older ones to the first one

    let survivor;
    let toInsert = [];
    let toDelete = [];
    res2.records.map((rec) => {
      if (!survivor) {
        survivor = rec;
      } else {
        rec.ContentDocumentLinks.records.map((cdl) => {
          toInsert.push({
            LinkedEntityId: cdl.LinkedEntityId,
            ContentDocumentId: survivor.Id,
            ShareType: cdl.ShareType,
            Visibility: cdl.Visibility,
          });
        });
        toDelete.push(rec.Id);
      }
    });
    debug({ toInsert });
    let insertErrors = await conn.insert("ContentDocumentLink", toInsert);
    insertErrors = insertErrors.filter(
      (x) => !x.success && x.errors[0].statusCode != "DUPLICATE_VALUE"
    );
    insertErrors.map(console.error);
    // de-dupe
    toDelete = [...new Set(toDelete)];
    if (insertErrors.length == 0) {
      debug({ toDelete });
      if (toDelete.length > 8) break;
      let deleteRes = await conn.delete("ContentDocument", toDelete);
      debug({ deleteRes });
    }
    // break;
  }
}

module.exports = mergeDuplicateFiles;
