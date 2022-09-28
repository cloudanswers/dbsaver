const debug = require("debug")("tasks");
const salesforce = require("../salesforce");
const cliProgress = require("cli-progress");
const { getCached, getCacheKey, query } = require("./cache");

async function bulkQuery(conn, soql) {
  return new Promise((resolve, reject) => {
    let records = [];
    conn.bulk
      .query(soql)
      .on("record", (rec) => records.push(rec))
      .on("error", reject)
      .on("end", () => resolve(records));
  });
}

async function run(connectionPrefix, tableName) {
  debug("processing ", connectionPrefix);
  const conn = await salesforce.getConnectionFromStorage(connectionPrefix);
  const id = await getCached(getCacheKey(conn, "identity"), () =>
    conn.identity()
  );
  debug(`processing ${id.username}`);

  let countRes = await conn.query(`select count(Id) from ${tableName}`);

  let bar = new cliProgress.SingleBar();
  bar.start(countRes.records[0].expr0);

  let promises = [];
  let lastId = "";
  while (true) {
    let soql = `SELECT Id from ${tableName} where IsDeleted = false and Id > '${lastId}' order by Id asc limit 10000`;
    let res = await bulkQuery(conn, soql);
    res = res.filter((x) => x.Id > lastId);
    bar.increment(res.length);
    if (!res.length) break;
    if (lastId == res[res.length - 1].Id)
      return debug(
        `queried same id twice, probably blocked deleting: ${lastId}`
      );
    lastId = res[res.length - 1].Id;
    // debug(`got ${res.length} records, lastId: ${lastId}`);
    let p = conn
      .sobject(tableName)
      .bulkload("delete", {}, res)
      .then((res) => res.filter((x) => !x.success).map(debug))
      .catch(console.error);
    promises.push(p);
  }

  // TODO backup before delete

  await Promise.all(promises);
  debug("Done.");
}

module.exports = run;
