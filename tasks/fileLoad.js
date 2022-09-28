const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const salesforce = require("../salesforce");
const storage = require("../storage");
const { getCached, getCacheKey, query } = require("./cache");

async function getConnection(connectionPrefix) {
  let keyPrefix = path.join(connectionPrefix, "__auth/");
  // get last key by iterating to the end
  let res;
  for await (const authKey of storage.list(keyPrefix)) {
    res = authKey;
  }
  return salesforce.getConnectionFromStorage(res);
}

async function describeObject(conn, sobjectName) {
  const cacheKey = getCacheKey(conn, sobjectName, "describe");
  return getCached(cacheKey, () => conn.describe(sobjectName));
}

function walk(dir, res = []) {
  fs.readdirSync(dir).map((f) => {
    f = path.join(dir, f);
    fs.statSync(f).isDirectory() ? walk(f, res) : res.push(f);
  });
  return res;
}

async function describeGlobal(conn) {
  return getCached(getCacheKey(conn, "describeGlobal"), () =>
    conn.describeGlobal()
  );
}

async function run(connectionPrefix1) {
  return console.error("DO NOT RUN THIS UNLESS YOU KNOW WHAT YOU'RE DOING");
  debug("processing ", connectionPrefix1);
  const conn = await getConnection(connectionPrefix1);
  let { sobjects } = await describeGlobal(conn);
  let cdlFields = (
    await describeObject(conn, "ContentDocumentLink")
  ).fields.map((x) => x.name);
  debug("loading dest ids...");
  const EXTERNAL_ID_FIELD = "Mi9_ID__c";
  const ID_MAPPING = {};

  let promises = sobjects
    .filter((x) => x.queryable && x.createable)
    .map(async (obj) => {
      debug("Processing " + obj.name);
      let desc = await describeObject(conn, obj.name);
      let idField = desc.fields
        .map((x) => x.name)
        .filter((x) => x.toLowerCase() == EXTERNAL_ID_FIELD.toLowerCase())[0];
      if (!idField) return;
      let lastId = "";
      for (let i = 0; i < 1_000; i++) {
        debug(`querying ${obj.name}`);
        let { records } = await query(
          conn,
          `select Id, ${idField}, ` +
            `(select ${cdlFields.join(
              ","
            )},ContentDocument.Title from ContentDocumentLinks) ` +
            `from ${obj.name} where ${idField} != '' ` +
            `and Id > '${lastId}' order by Id asc limit 200`,
          false
        );
        debug(`found ${records.length} ${obj.name} records`);
        if (records.length < 1) break;
        records.map((x) => {
          // console.log({ x });
          ID_MAPPING[x[idField]] = x;
          lastId = x.Id;
        });
      }
    });
  promises = promises.map((p) => p.catch((e) => e));
  debug("********** AWAITING PROMISES **********");
  await Promise.all(promises).catch(console.error);

  let files = walk("./attachments_export");
  console.log({ files });

  for (const f of files) {
    if (!f.endsWith(".meta.json")) continue;
    debug(`processing ${f}`);
    let metaRaw = fs.readFileSync(f, { encoding: "utf-8" }).toString();
    let meta = JSON.parse(metaRaw);
    for (const k of Object.keys(ID_MAPPING)) {
      if (!metaRaw.includes(k)) continue;
      debug("found needed file", f, k);
      debug(meta);

      const existingFiles = ID_MAPPING[k].ContentDocumentLinks?.records || [];
      const existingFileNames = existingFiles.map(
        (x) => x.ContentDocument.Title
      );

      debug("checking for existing...");
      debug({ existingFileNames });

      // check if the record already has this file and is the same size
      if (existingFileNames.includes(meta.Name || meta.Title)) {
        debug("SKIPPING FILE ALREADY LOADED");
        continue;
      }
      debug("uploading version...");
      let realFile = f.replace(".meta.json", "");
      let PathOnClient =
        path.basename(realFile) +
        (realFile.toLowerCase().endsWith(meta.FileExtension)
          ? ""
          : "." + meta.FileExtension);
      try {
        let res = await conn.sobject("ContentVersion").create({
          FirstPublishLocationId: ID_MAPPING[k].Id,
          Title: meta.Name || meta.Title,
          PathOnClient,
          VersionData: fs
            .readFileSync(f.replace(".meta.json", ""))
            .toString("base64"),
          // not writable
          // FileType: meta.FileType,
          // FileExtension: meta.FileExtension,
          Description: meta.Description,
        });
        debug({ res });
      } catch (e) {
        console.error(e);
      }
    }
  }
  debug("Done.");
}

module.exports = run;
