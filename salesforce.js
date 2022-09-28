const path = require("path");
const jsforce = require("jsforce");
const settings = require("./settings");
const storage = require("./storage");

const oauth2 = (loginUrl) =>
  new jsforce.OAuth2({
    clientId: settings.SALESFORCE_CLIENT_ID,
    clientSecret: settings.SALESFORCE_CLIENT_SECRET,
    redirectUri: settings.SALESFORCE_CALLBACK_URL,
    loginUrl,
  });

async function getConnectionFromStorage(key) {
  if (!key) throw new Error("missing key");

  // if it's just the connection path (folder), get the latest auth from within
  if (key.endsWith("/")) {
    // ensure in auth folder
    if (!key.endsWith("__auth/")) key = path.join(key, "__auth/");
    // iterate thru auth keys and save last one
    for await (const k of storage.list(key)) {
      key = k;
    }
  }

  const auth = JSON.parse(await storage.get(key));
  const conn = await getConnection(
    auth.instance_url,
    auth.access_token,
    auth.refresh_token
  );
  // conn.bulk.pollInterval = 5 * 1000; // 5 sec
  conn.bulk.pollTimeout = 60 * 60 * 1000; // one hour
  return conn;
}

async function getConnectionsFromStorage(opts) {
  const { limit } = opts || {};
  let conns = [];
  let orgKeys = [];
  for await (const key of storage.list("salesforce/")) {
    if (key == "salesforce/") continue;
    if (!key.endsWith("/")) continue;
    orgKeys.push(key);
  }
  // get user keys
  let promiseBuffer = orgKeys.map(async (orgPrefix) => {
    for await (const userPrefix of storage.list(orgPrefix)) {
      conns.push({ key: userPrefix, id: {} });
    }
  });
  await Promise.all(promiseBuffer);

  if (limit && limit < conns.length) conns = conns.slice(0, limit);
  const scanOrgFolder = async (conn) => {
    if (!conn.key) return console.error("missing key:", conn);
    // loop through users to get user info
    try {
      conn.id = JSON.parse(
        await storage.get(path.join(conn.key, "__id/latest"))
      );
    } catch (e) {
      console.debug("caching id for ", conn.key);
      let lastKey;
      for await (const kkk of storage.list(conn.key + "__id/")) {
        lastKey = kkk;
      }
      conn.id = JSON.parse(await storage.get(lastKey));
      // cache in the "latest" file so it's faster next time
      storage.put(path.join(conn.key, "__id/latest"), conn.id);
    }
  };
  promiseBuffer = conns.map((x) => scanOrgFolder(x).catch((e) => e));
  await Promise.all(promiseBuffer);
  return conns;
}

async function getConnection(instanceUrl, accessToken, refreshToken) {
  return new jsforce.Connection({
    oauth2: {
      clientId: settings.SALESFORCE_CLIENT_ID,
      clientSecret: settings.SALESFORCE_CLIENT_SECRET,
      redirectUri: settings.SALESFORCE_CALLBACK_URL,
      loginUrl: instanceUrl,
    },
    instanceUrl,
    accessToken,
    refreshToken,
    version: "53.0",
  });
}

/**
 *
 * @param {jsforce.Connection} conn
 * @returns {string} url to start browser session
 */
async function frontdoor(conn) {
  // ensure we are logged in and access token is refreshed
  await conn.identity();
  return conn.instanceUrl + "/secur/frontdoor.jsp?sid=" + conn.accessToken;
}

module.exports = {
  getConnection,
  getConnectionFromStorage,
  getConnectionsFromStorage,
  frontdoor,
  oauth2,
};
