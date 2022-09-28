const AWS = require("aws-sdk");
const debug = require("debug")("storage");
const settings = require("./settings");
const zlib = require("zlib");

const s3 = new AWS.S3({
  endpoint: new AWS.Endpoint(settings.AWS_ENDPOINT),
  accessKeyId: settings.AWS_ACCESS_KEY_ID,
  secretAccessKey: settings.AWS_SECRET_ACCESS_KEY,
  signatureVersion: "v4",
});

const bb = s3;

async function* listFiles(Prefix, Delimiter = "/", backend = s3) {
  let params = {
    Bucket: backend == s3 ? settings.AWS_S3_BUCKET : settings.BB_S3_BUCKET,
    Prefix,
    Delimiter,
  };
  while (true) {
    let keys = [];
    let res = await backend.listObjectsV2(params).promise();
    if (res.Contents) keys = keys.concat(res.Contents);
    if (res.CommonPrefixes) keys = keys.concat(res.CommonPrefixes);
    for (let key of keys) yield key;
    if (res.NextContinuationToken)
      params.ContinuationToken = res.NextContinuationToken;
    else break;
  }
}

async function* list(Prefix, Delimiter = "/", backend = s3) {
  let params = {
    Bucket: backend == s3 ? settings.AWS_S3_BUCKET : settings.BB_S3_BUCKET,
    Prefix,
    Delimiter,
  };

  while (true) {
    let keys = [];
    debug("listing objects...");
    let res = await backend.listObjectsV2(params).promise();
    debug({ res, CommonPrefixes: res.CommonPrefixes });
    if (res.Contents) keys = keys.concat(res.Contents.map((k) => k.Key));
    if (res.CommonPrefixes)
      keys = keys.concat(res.CommonPrefixes.map((k) => k.Prefix));
    for (let key of keys) yield key;
    if (res.NextContinuationToken)
      params.ContinuationToken = res.NextContinuationToken;
    else break;
  }
}

async function put(key, content, jsonStringify = true, backend = s3) {
  return backend
    .putObject({
      Bucket: backend == s3 ? settings.AWS_S3_BUCKET : settings.BB_S3_BUCKET,
      Key: key,
      Body: zlib.gzipSync(jsonStringify ? JSON.stringify(content) : content),
      ContentEncoding: "gzip",
    })
    .promise();
}

async function set(key, body) {
  return put(key, body);
}

async function exists(key, backend = s3) {
  return backend
    .headObject({
      Key: key,
      Bucket: backend == s3 ? settings.AWS_S3_BUCKET : settings.BB_S3_BUCKET,
    })
    .promise()
    .then(() => true)
    .catch(() => false);
}

async function get(key, backend = s3) {
  var res = await backend
    .getObject({
      Key: key,
      Bucket: backend == s3 ? settings.AWS_S3_BUCKET : settings.BB_S3_BUCKET,
    })
    .promise();
  // large text bodies are gzipped to save space
  // so we first try to gunzip in case it's that, otherwise assume it's a buffer
  // FIXME this expects strings, so will not work for zips
  try {
    return zlib.gunzipSync(res.Body).toString("utf-8");
  } catch (e) {
    debug("falling back to raw body");
    return res.Body.toString("utf-8");
  }
}

async function getJson(key, backend = s3) {
  return JSON.parse(await get(key, backend));
}

async function del(key, backend = s3) {
  return backend
    .deleteObject({
      Key: key,
      Bucket: backend == s3 ? settings.AWS_S3_BUCKET : settings.BB_S3_BUCKET,
    })
    .promise();
}

module.exports = {
  set,
  put,
  get,
  del,
  list,
  listFiles,
  exists,
  s3,
  bb,
  getJson,
};
