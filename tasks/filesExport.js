const Bottleneck = require("bottleneck/es5");
const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const salesforce = require("../salesforce");
const storage = require("../storage");
const fetch = require("node-fetch");

const limiter = new Bottleneck({
  maxConcurrent: 10,
});

debug("Loading cache...");
let cache = {};
const CACHE_FOLDER = "./cache/";
if (fs.existsSync(CACHE_FOLDER)) {
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
  fs.mkdirSync(CACHE_FOLDER);
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
  if (cache[key]) return cache[key];
  let val = await promiseToResolve();
  cache[key] = val;
  let cacheFile = path.join(CACHE_FOLDER, new Date().getTime() + ".json");
  fs.writeFile(cacheFile, JSON.stringify({ [key]: val }), () => {});
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

/**
 * not using jsforce file blob().pipe() because getting random exits
 * @param {*} conn
 * @param {*} obj
 * @param {*} id
 * @param {*} field
 */
async function downloadToBuffer(conn, obj, id, field, outFile) {
  let url = [conn._baseUrl(), "sobjects", obj, id, field].join("/");
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${conn.accessToken}` },
  });
  debug(` saving: ${outFile}`);
  fs.writeFileSync(outFile, await res.buffer());
  debug(` saved`, outFile, fs.statSync(outFile).size);
}

async function* queryAll(conn, soql) {
  let records;
  let lastId = "";
  const ID_FIELD = "Id";
  while (!records || records.length > 0) {
    debug(`downloading chunk after ${lastId}`);
    let offsetFilter = lastId
      ? (soql.toLowerCase().includes(" where ") ? " and " : " where ") +
        ` ${ID_FIELD} > '${lastId}' `
      : "";
    let paginatedSoql =
      soql + ` ${offsetFilter} order by ${ID_FIELD} asc limit 1000 `;
    debug(paginatedSoql);
    let res = await query(conn, paginatedSoql);
    let { records } = res;
    debug({ res });
    if (records.length == 0) {
      debug("done downloading");
      debug({ res });
      break;
    }
    for (const r of records) {
      yield r;
      lastId = r[ID_FIELD];
    }
  }
}

async function filesExport(connectionPrefix) {
  debug("processing ", connectionPrefix);
  const conn = await getConnection(connectionPrefix);

  const outputFolder = "attachments_export/" + connectionPrefix;
  debug(`outputFolder: ${outputFolder}`);
  fs.mkdirSync(outputFolder, { recursive: true });

  let { sobjects } = await conn.describeGlobal();

  // lightning files

  // this is a crazy query that doesn't work in bulk
  // but there are query restrictions on the ContentDocumentLink and ContentVersion

  let cd_soql = await conn.sobject("ContentDocument").find().toSOQL();
  // " USING SCOPE everything ";
  let cv_soql = await conn.sobject("ContentVersion").find().toSOQL();
  let cdl_soql = await conn.sobject("ContentDocumentLink").find().toSOQL();
  // // insert subqueries
  cd_soql = cd_soql.replace("SELECT Id,", `SELECT Id, (${cv_soql}s),`);
  cd_soql = cd_soql.replace("SELECT Id,", `SELECT Id, (${cdl_soql}s),`);
  // // add reference to linked entity so we don't have to query for it later
  cd_soql = cd_soql.replace(
    "LinkedEntityId,",
    "LinkedEntityId, LinkedEntity.Name,"
  );
  cd_soql = cd_soql.replace("VersionData,", ""); // can't query this directly

  let downloadContentDocument = async (contentDocument) => {
    for (const cdl of contentDocument.ContentDocumentLinks.records) {
      const idPrefix = cdl.LinkedEntityId.substr(0, 3);
      // Organization
      if (idPrefix == "00D") continue;
      // ListEmail
      if (idPrefix == "0XB") continue;
      const sobj = sobjects.filter((x) => x.keyPrefix == idPrefix)[0];
      if (!sobj) {
        debug(`skipping sobj not found: ${idPrefix}`);
        continue;
      }
      // skip users because they just get a double copy of everything?
      if (sobj.name == "User") continue;
      let recordFolder = path.join(
        outputFolder,
        sobj.name.replace(/\//gs, "_"),
        cdl.LinkedEntity.Name.replace(/\//gs, "_")
      );
      fs.mkdirSync(recordFolder, { recursive: true });
      const outFile = path.join(
        recordFolder,
        contentDocument.Title.replace(/\//gs, "_")
      );
      if (
        fs.existsSync(outFile) &&
        // sometimes it's 100 bytes off... why??
        // this checks if we're at 90% of size, hoping it's an encoding issue that causes a diff in size
        fs.statSync(outFile).size / parseInt(contentDocument.ContentSize) > 0.9
      ) {
        // debug(`  skipping same size: ${outFile}`);
        continue;
      }

      debug("downloading", outFile, `(${contentDocument.ContentSize})`);

      fs.writeFileSync(
        outFile + ".meta.json",
        JSON.stringify(contentDocument, undefined, 2)
      );

      return downloadToBuffer(
        conn,
        "ContentVersion",
        contentDocument.LatestPublishedVersionId,
        "VersionData",
        outFile
      );
    }
  };

  debug("finding ContentDocument Ids from ContentDocumentLink records...");
  let cdIds = [];
  let promises = [];
  for await (const r of queryAll(
    conn,
    cdl_soql + ` where LinkedEntityId in (select Id from User) `
  )) {
    cdIds.push(r.ContentDocumentId);
  }
  // dedupe
  cdIds = [...new Set(cdIds)];
  let cdIdsChunked = chunk(cdIds, 500);
  debug(`start ContentDocument download (${cdIds.length})...`);
  await Promise.all(
    cdIdsChunked.map(
      limiter.wrap(async (x) => {
        const { records } = await query(
          conn,
          cd_soql + ` where Id in (${x.map((y) => `'${y}'`).join(",")}) `
        );
        if (!records.length) return debug(`NOT FOUND ContentDocument: ${x}`);
        for (const cd of records) {
          debug("  processing", cd.Id);
          await downloadContentDocument(cd);
        }
      })
    )
  );
  debug(cdIds.length);
  // process.exit();

  // process Attachment

  debug("downloading attachments...");
  let soql = await conn.sobject("Attachment").find().toSOQL();

  // remove binary data from query
  // add parent object name so we don't need to query for it
  soql = soql.replace("Body,", "Parent.Name,");

  // group by parentid to make it more efficient to query related names
  // filter out empty files (yes, some exist)
  soql = soql + " where ParentId != null and BodyLength > 0";

  const downloadAttachment = async (record) => {
    debug(record.Id);
    let sobj = sobjects.filter(
      (x) => x.keyPrefix == record.ParentId.substr(0, 3)
    )[0]; // FIXME likely null pointer exception
    let recordFolder = path.join(
      outputFolder,
      sobj.name.replace(/\//gs, "_"),
      record.Parent ? record.Parent.Name.replace(/\//gs, "_") : record.ParentId
    );
    fs.mkdirSync(recordFolder, { recursive: true });
    const outFile = path.join(recordFolder, record.Name.replace(/\//gs, "_"));
    if (
      fs.existsSync(outFile) &&
      // sometimes it's 100 bytes off... why??
      // this checks if we're at 90% of size, assuming it's an encoding issue
      fs.statSync(outFile).size / parseInt(record.BodyLength) > 0.9
    ) {
      debug(`  skipping same size: ${outFile}`);
      return;
    }

    fs.writeFileSync(
      outFile + ".meta.json",
      JSON.stringify(record, undefined, 2)
    );
    return downloadToBuffer(conn, "Attachment", record.Id, "Body", outFile);
  };

  for await (const r of queryAll(conn, soql)) {
    await downloadAttachment(r);
  }

  // return Promise.all(records.map(limiter.wrap(downloadAttachment)));
}

module.exports = filesExport;
