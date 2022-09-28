const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const salesforce = require("../salesforce");
const XLSX = require("xlsx");
const stringSimilarity = require("string-similarity");
const { getCached, getCacheKey, clearCache } = require("./cache");
const { log } = console;

const BAD_OBJECTS = [
  "Announcement",
  "AppTabMember",
  "ColorDefinition",
  "ContentDocumentLink",
  "ContentFolderItem",
  "ContentFolderMember",
  "DataType",
  "DatacloudAddress",
  "EmbeddedServiceDetail",
  "EmbeddedServiceLabel",
  "EntityParticle",
  "FeedAttachment",
  "FeedComment",
  "FeedItem",
  "FeedRevision",
  "FieldDefinition",
  "FormulaFunction",
  "ApexTestResult",

  // SKIPPING FOR NOW
  "CaseFeed",
  "CaseHistory",
  "CaseShare",
  "EmailMessage",
  "EmailMessageRelation",
  "FieldPermissions",
  "DuplicateRecordItem",
  "CampaignMember",
];

// async function retrieve(conn, sobj, id) {
//   const cacheKey = getCacheKey(conn, path.join("retrieve", sobj, id));
//   return getCached(cacheKey, () => conn.sobject(sobj).retrieve(id));
// }

async function query(conn, soql) {
  debug(`soql=${soql}`);
  const cacheKey = getCacheKey(conn, soql);
  let res = await getCached(cacheKey, () => conn.query(soql));
  debug("query returned");
  // don't cache empty results because we'll want to fill them in later
  // or if we're paginating through, and a new record comes in the next day, the cache will prevent us from getting it
  if (!res.records || res.records.length < 1) {
    debug("cleaning up empty cached value");
    clearCache(cacheKey);
  }
  return res;
}

async function* queryAllGen(conn, soql) {
  const limit = 2000;
  let records;
  let lastId = "";
  const ID_FIELD = "Id";
  while (!records || records.length > 0) {
    let offsetFilter = lastId
      ? (soql.toLowerCase().includes("where ") ? "and " : " where ") +
        ` ${ID_FIELD} > '${lastId}' `
      : "";
    let paginatedSoql =
      soql.trim() + ` ${offsetFilter} order by ${ID_FIELD} asc limit ${limit} `;
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

async function spreadsheetCompare(
  connectionPrefix1,
  connectionPrefix2,
  filter
) {
  log("processing ", connectionPrefix1, connectionPrefix2, filter);
  const [conn1, conn2] = await Promise.all([
    salesforce.getConnectionFromStorage(connectionPrefix1),
    salesforce.getConnectionFromStorage(connectionPrefix2),
  ]);
  const outputFolder =
    "output/" +
    connectionPrefix1.replace(/\//g, "_") +
    "___" +
    connectionPrefix2.replace(/\//g, "_") +
    "___" +
    new Date()
      .toISOString()
      .substring(0, 10)
      .replace(/[\D\W\-]/g, "_");
  log({ outputFolder });
  fs.mkdirSync(outputFolder, { recursive: true });

  console.log(conn1.limitInfo, conn2.limitInfo);

  let [conn1sobjects, conn2sobjects] = await Promise.all([
    getCached("describeGlobal", () => conn1.describeGlobal()).then(
      (x) => x.sobjects
    ),
    getCached("describeGlobal", () => conn2.describeGlobal()).then(
      (x) => x.sobjects
    ),
  ]);

  let summary = {};
  for (const sobj1 of conn1sobjects) {
    if (fs.existsSync(path.join(outputFolder, `${sobj1.name}.xlsx`))) continue;
    if (!sobj1.queryable) continue;
    if (BAD_OBJECTS.includes(sobj1.name)) continue;
    if (filter && sobj1.name != filter) continue;

    let sobj2;
    conn2sobjects
      .filter((x) => x.createable)
      .filter((x) => x.name.toLowerCase() == sobj1.name.toLowerCase())
      .map((x) => (sobj2 = x));

    log(sobj1.name, "==>", sobj2?.name);
    if (!sobj2) {
      log("skipping, no dest table");
      summary[sobj1.name] = "missing dest table";
      continue;
    }

    // skip empty tables
    debug(`getting count...`);
    const soql = `select count(Id) TOTAL_COUNT from ${sobj1.name}`;
    const {
      records: [countRes],
    } = await query(conn1, soql).catch((e) => {
      debug(e);
      return { records: [{ TOTAL_COUNT: 0 }] };
    });
    debug(`[${sobj1.name}] count=${countRes.TOTAL_COUNT}`);
    if (countRes.TOTAL_COUNT < 1) {
      debug(`[${sobj1.name}] skipping empty table`);
      continue;
    }

    debug(`describing source fields on ${sobj1.name}...`);
    let { fields: fields1 } = await getCached(
      getCacheKey(conn1, sobj1.name, "describe"),
      () => conn1.describe(sobj1.name)
    );

    debug(`describing dest fields on ${sobj2.name}...`);
    debug(sobj2);
    let sobj2desc = await getCached(
      getCacheKey(conn2, sobj2.name, "describe"),
      async () => conn2.describe(sobj2.name)
    ).catch((e) => {
      debug(`error getting dest object fields`);
      return { fields: [] };
    });
    debug({ sobj2desc });
    let { fields: fields2 } = sobj2desc;
    debug(fields2);

    if (!fields2.map((f) => f.name).includes("NRF_External_ID__c")) {
      debug("skipping table with no external id");
      continue;
    }

    let fieldsStatus = {};
    let fieldsToQuery1 = [];
    for (const f1 of fields1) {
      // skip fields that are read-only in source system
      if (f1.name != "Id" && !f1.updateable) continue;
      let matched;
      for (const f2 of fields2) {
        if (f1.name == f2.name) {
          matched = f2;
        }
      }
      let status;
      if (matched) {
        if (matched.updateable) {
          fieldsToQuery1.push(f1.name);
          status = "ok";
        } else status = "readonly";
      } else {
        status = "field missing";
      }
      fieldsStatus[f1.name] = status;
    }

    log({ fieldsStatus });

    if (fieldsToQuery1.length < 1) {
      log("no fields to copy");
      continue;
    }

    fieldsToQuery1.sort();

    debug("starting big queries...");
    let records1 = queryAllGen(
      conn1,
      `select Id, ${fieldsToQuery1} from ${sobj1.name}`
    );
    let records2 = [];
    for await (const row of queryAllGen(
      conn2,
      `select Id,NRF_External_ID__c,${fieldsToQuery1} from ${sobj1.name} where NRF_External_ID__c != ''`
    )) {
      records2.push(row);
    }
    debug("Getting destination records... Done.");
    let diffs = [];
    let fieldDiffs = [];
    for await (const rec1 of records1) {
      delete rec1.attributes;
      Object.keys(rec1).map((f) => rec1[f] || delete rec1[f]);
      let rec2 = {};
      for (const _ of records2) {
        if (_.NRF_External_ID__c == rec1.Id) {
          rec2 = _;
          break;
        }
      }

      // let rec2 = await retrieve(
      //   conn2,
      //   sobj2.name,
      //   "NRF_External_ID__c/" + rec1.Id
      // );

      // remove blanks

      delete rec2.attributes;
      Object.keys(rec2).map((f) => rec2[f] || delete rec2[f]);

      // log({ rec2 });

      // compare

      let diff1 = {};
      Object.keys(rec1).map((f1) => {
        let diffScore = stringSimilarity.compareTwoStrings(
          new String(rec1[f1]).trim(),
          new String(rec2[f1]).trim()
        );
        if (diffScore < 1 && f1 != "Id") {
          diff1[f1] = rec1[f1];
          diff1[f1 + "_2"] = rec2[f1];
          diff1[f1 + "_score"] = diffScore;
          fieldDiffs.push({
            id: rec1.Id,
            f: f1,
            diff: diffScore,
            source: rec1[f1],
            dest: rec2[f1],
          });
        }
      });

      diffs.push(diff1);
    }

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(
        Object.keys(fieldsStatus).map((f) => [f, fieldsStatus[f]])
      ),
      "fieldsStatus"
    );
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(diffs), "diffs");
    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(fieldDiffs),
      "fieldDiffs"
    );
    XLSX.writeFile(wb, path.join(outputFolder, `${sobj1.name}.xlsx`));

    // break;
  }
}

module.exports = spreadsheetCompare;
