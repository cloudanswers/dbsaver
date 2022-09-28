const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const md5 = require("md5");
const { USE_CACHE } = process.env;

debug("loading cache...");
let cache = {};
const CACHE_FOLDER = "./cache/";
if (USE_CACHE && fs.existsSync(CACHE_FOLDER)) {
  fs.readdirSync(CACHE_FOLDER).map((f) => {
    const fullPath = path.join(CACHE_FOLDER, f);
    const x = fs.readFileSync(fullPath, "utf-8");
    try {
      // store pointer to where the data is, otherwise we run out of memory
      Object.keys(JSON.parse(x)).map((k) => (cache[k] = fullPath));
    } catch (e) {
      //ignore file errors
    }
  });
} else {
  fs.mkdirSync(CACHE_FOLDER, { recursive: true });
}
debug("loading cache done.");

async function getCached(key, promiseToResolve) {
  if (USE_CACHE && cache[key]) {
    // debug(`[cache] returning cached version`);
    let val = JSON.parse(fs.readFileSync(cache[key], "utf-8"))[key];
    if (val) return val;
  }
  let val = await promiseToResolve();
  if (USE_CACHE) {
    // debug(`[cache] setting cached value`);
    let cacheFile = path.join(CACHE_FOLDER, new Date().getTime() + ".json");
    fs.writeFileSync(cacheFile, JSON.stringify({ [key]: val }));
    cache[key] = cacheFile;
  }
  return val;
}

function getCacheKey(conn, ...key) {
  const orgId = conn?.accessToken.split("!")[0];
  if (!orgId) throw new Error("missing accessToken");
  let res = [orgId, ...key].join("/");
  return md5(res);
}

function isCached(key) {
  return !!cache[key];
}

function clearCache(key) {
  // cache a null value
  getCached(key, async () => null);
}

async function query(conn, soql, useCache = true) {
  const cacheKey = getCacheKey(conn, soql);
  return useCache
    ? getCached(cacheKey, () => conn.query(soql))
    : conn.query(soql);
}

module.exports = {
  query,
  getCacheKey,
  getCached,
  clearCache,
  isCached,
};
