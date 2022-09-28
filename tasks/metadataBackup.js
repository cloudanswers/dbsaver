const fs = require("fs");
const debug = require("debug")("tasks");
const path = require("path");
const salesforce = require("../salesforce");
const storage = require("../storage");

async function getConnection(connectionPrefix) {
  let res;
  for await (const authKey of storage.list(
    path.join(connectionPrefix, "__auth/")
  )) {
    res = authKey;
  }
  return salesforce.getConnectionFromStorage(res);
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function run(connectionPrefix) {
  debug("processing ", connectionPrefix);
  const conn = await getConnection(connectionPrefix);
  let res = await conn.metadata.describe();
  let xmlNames = res.metadataObjects.map((x) => x.xmlName);
  debug({ xmlNames });
  let types = [];

  const processType = async (name) => {
    debug(name);
    let t = { name, members: [] };
    let res2;
    try {
      //   debug(`[${name}] listing md...`);
      res2 = await conn.metadata.list([{ type: name, folder: null }], "53.0");
      //   debug(`[${name}] done listing md`);
    } catch (e) {
      debug(`[${name}] EXCEPTION: `, e);
      return;
    }
    // debug(`${name} res2:`, res2);

    if (!res2) return;
    if (!res2.length) res2 = [res2];
    for (const i of res2) {
      t.members.push(i.fullName);
    }
    types.push(t);
  };

  await Promise.all(xmlNames.map(processType));

  let retrieveRes = await conn.metadata.retrieve({
    apiVersion: "53.0",
    singlePackage: true,
    unpackaged: {
      types,
    },
  });

  while (true) {
    debug("retrieveRes: ", retrieveRes);
    debug(typeof retrieveRes.done);
    if (retrieveRes.done === true) break;
    if (retrieveRes.done === "true") break;
    await sleep(5000);
    debug("checking...");
    retrieveRes = await conn.metadata.checkRetrieveStatus(retrieveRes.id);

    debug("check done");
  }

  let filename = `./out-${new Date().getTime()}.json`;
  fs.writeFileSync(filename, JSON.stringify(retrieveRes, undefined, 2));
  let buff = new Buffer(retrieveRes.zipFile, "base64");
  fs.writeFileSync(filename + ".zip", buff);

  debug("retrieveRes: ", retrieveRes);
}

module.exports = run;
