const cliProgress = require("cli-progress");
const Bottleneck = require("bottleneck/es5");
const debug = require("debug")("tasks");
const path = require("path");
const storage = require("../storage");
const salesforce = require("../salesforce");
const md5 = require("md5");
const { getCacheKey, getCached } = require("./cache");

const BAD_OBJECTS = [];

const limiter = new Bottleneck({
  maxConcurrent: process.env.MAX_CONCURRENT
    ? parseInt(process.env.MAX_CONCURRENT)
    : 10,
});

async function query(conn, soql) {
  return limiter.schedule(() => conn.query(soql));
}

async function describeGlobal(conn) {
  return getCached(getCacheKey(conn, "describeGlobal"), () =>
    conn.describeGlobal()
  );
}

async function storagePut(key, data) {
  return limiter.schedule(() => storage.put(key, data));
}

// async function describeObject(conn, sobjectName) {
//   return getCached(
//     getCacheKey(conn, "describe", sobjectName),
//     limiter.schedule(() => conn.describe(sobjectName))
//   );
// }

// async function retrieve(conn, sobj, id) {
//   const cacheKey = getCacheKey(conn, path.join("retrieve", sobj, id));
//   return getCached(cacheKey, () => conn.sobject(sobj).retrieve(id));
// }

async function* queryAll(conn, soql, lastId = "") {
  const tableName = soql.split(" FROM ")[1].split(" ")[0];
  const limit = 5000;
  let records;
  const ID_FIELD = "Id";
  while (!records || records.length > 0) {
    if (lastId) debug(`downloading ${tableName} chunk after ${lastId}`);
    let offsetFilter = lastId
      ? (soql.toLowerCase().includes(" where ") ? " and " : " where ") +
        ` ${ID_FIELD} > '${lastId}' `
      : "";
    let paginatedSoql =
      soql + ` ${offsetFilter} order by ${ID_FIELD} asc limit ${limit} `;
    let res = await query(conn, paginatedSoql);
    records = res.records;
    for (const r of records) {
      lastId = r[ID_FIELD];
      yield r;
    }
    if (res.records.length < 1) {
      break;
    }
  }
}

async function processObject(prefix, conn, sobjName) {
  const storageKeyFolder = path.join("backup", prefix, "sobjects", sobjName);
  let lastId = "";
  for await (const k of storage.list(storageKeyFolder + "/")) {
    if (path.basename(k).length == 18) lastId = path.basename(k);
  }
  let soql = await conn.sobject(sobjName).find().toSOQL();
  // limited to 200 per chunk, which is much slower:
  // let soql = `select fields(all) from ${sobjName}`;
  let promises = [];
  for await (const row of queryAll(conn, soql, lastId)) {
    const storageKey = path.join(storageKeyFolder, row.Id);
    promises.push(storagePut(storageKey, row));
    // const storageKeyDate = path.join(
    //   storageKey + '__HISTORY',
    //   row.SystemModstamp || row.LastModifiedDate
    // );
    // promises.push(storage.put(storageKeyDate, row));
    // chip away at the buffer before it reaches 100k promises and we run out of memory
    // TODO move to a callback or worker pool model
    if (promises.length >= 1000) {
      await Promise.all(promises);
      promises = [];
    }
  }
  await Promise.all(promises);
}

/**
 * provides function you can use like this:
 *   [...].map(get('name'))
 * @param {*} prop
 * @returns {*}
 */
function get(prop) {
  return (input) => input[prop];
}

/**
 *
 * @param {*} connectionPrefix
 */
async function saveToS3(connectionPrefix) {
  debug("processing ", connectionPrefix);
  const conn = await salesforce.getConnectionFromStorage(connectionPrefix);
  const { sobjects } = await describeGlobal(conn);
  let promises = [];
  for (const sobj of sobjects) {
    if (!sobj.queryable) continue;
    promises.push(processObject(connectionPrefix, conn, sobj.name));
  }
  await Promise.all(promises);
  debug("Done.");
}

module.exports = saveToS3;
