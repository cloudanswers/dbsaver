const cliProgress = require("cli-progress");
const Bottleneck = require("bottleneck/es5");
const debug = require("debug")("tasks");
const path = require("path");
const salesforce = require("../salesforce");
const { getCached, getCacheKey, clearCache } = require("./cache");
const md5 = require("md5");

function newBar(name) {
  return new cliProgress.SingleBar({
    etaBuffer: 100_000,
    fps: 1,
    format:
      name + " [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} {id}",
  });
}
// without __c so we can use it in labels
const EXTERNAL_ID_FIELD = "Replication_External_ID";

const BAD_OBJECTS = [
  "Individual",
  "AuthorizationForm",
  "AuthorizationFormConsent",
  "AssociatedLocation",
  "ActionLinkGroupTemplate",
  "ActionLinkTemplate",
  "AuthorizationFormDataUse",
  "BusinessHours",
  "AuthorizationFormText",
  "CampaignMemberStatus",
  "CommSubscription",
  "CommSubscriptionChannelType",
  "CommSubscriptionTiming",
  "DuplicateRecordSet",
  "DataUseLegalBasis",
  "EmailTemplate",
  "EngagementChannelType",
  "EmailMessage",
  "EnhancedLetterhead",
  "ExternalEvent",
  "IPAddressRange",
  "User",
  "Group",
  "Organization",
  "ContentVersion",
  "ContentDocument",
  "ContentDocumentLink",
  "FeedItem",
  "Note",
  "npsp__Trigger_Handler__c",
  "ListEmail",
  "Holiday",
  "SBQQ__ColumnMetadata__c",
  "SBQQ__InstallProcessorLog__c",
  "SBQQ__RecordJob__c",
  "SBQQ__QuoteLine__c",
  "SBQQ__Quote__c",
];

const limiter = new Bottleneck({
  maxConcurrent: 10,
});

async function query(conn, soql) {
  debug(`query.soql=${soql}`);
  const cacheKey = getCacheKey(conn, soql);
  let res = await limiter.schedule(() =>
    getCached(cacheKey, () => conn.queryAll(soql))
  );
  if (!res) {
    console.error({ res, soql });
    process.exit();
  }
  // don't cache empty results because we'll want to fill them in later
  // or if we're paginating through, and a new record comes in the next day, the cache will prevent us from getting it
  if (res.records.length < 1) clearCache(cacheKey);
  return res;
}

async function ensureExternalIdField(conn, objectName) {
  // FIXME: name/prefix it after the source org id so we know where it came from
  debug(`[${objectName}] adding external id field to object...`);

  const field = `${objectName}.${EXTERNAL_ID_FIELD}__c`;
  const fieldDef = {
    fullName: field,
    label: "Replication External ID",
    type: "Text",
    length: 18,
    externalId: true,
    inlineHelpText: "ID from the source system.",
  };

  const { fields: existingFields } = await describeObject(conn, objectName);
  if (
    existingFields.filter((x) => x.name == EXTERNAL_ID_FIELD + "__c").length > 0
  ) {
    debug(`[${objectName}] external id field already exists, skipping`);
    return field;
  }

  let res = await limiter.schedule(() =>
    conn.metadata.upsert("CustomField", [fieldDef])
  );
  if (res.success) {
    debug(`  [${objectName}] success, adding to permission set...`);
    return field;
  } else {
    debug(`  [${objectName}] error: ${JSON.stringify(res)}`);
    // debug("OBJECT DESCRIBE:", await describeObject(conn, objectName));
    throw new Error(`error creating field: ${JSON.stringify(res)}`);
  }
}

async function addFieldsToPermissionSet(conn, fields) {
  let permissionSet = await conn.metadata.read(
    "PermissionSet",
    EXTERNAL_ID_FIELD
  );
  // debug("permissionSet:", permissionSet);
  permissionSet.label = permissionSet.label || EXTERNAL_ID_FIELD;
  permissionSet.fullName = permissionSet.fullName || EXTERNAL_ID_FIELD;
  permissionSet.fieldPermissions = permissionSet.fieldPermissions || [];
  fields.map((f) =>
    permissionSet.fieldPermissions.push({
      editable: "true",
      field: f,
      readable: "true",
    })
  );
  let res = await conn.metadata.upsert("PermissionSet", [permissionSet]);
  if (!res.success) {
    throw new Error(
      `error adding field to permission set: ${JSON.stringify(res)}`
    );
  }
}

async function describeGlobal(conn) {
  const cacheKey = getCacheKey(conn, "describeGlobal");
  return getCached(cacheKey, () => conn.describeGlobal());
}

async function describeObject(conn, sobjectName) {
  const cacheKey = getCacheKey(conn, sobjectName, "describe");
  return getCached(cacheKey, () => conn.sobject(sobjectName).describe());
}

function upsertCacheKey(destConn, destRecord) {
  if (!destRecord) throw new Error(`destRecord ${destRecord}`);
  return getCacheKey(
    destConn,
    "upsert",
    md5(
      // FIXME sort keys so key order won't matter to cache
      JSON.stringify(destRecord, undefined, 2).split(/\n/).sort().join("\n")
    )
  );
}

const UPSERT_CHUNK_SIZE = 1;

class UpsertManager {
  recordsToUpsert = [];
  recordsUpserted = [];
  upsertRes = [];

  constructor(destConn, sobjectName) {
    this.destConn = destConn;
    this.sobjectName = sobjectName;
  }

  async addRecord(record) {
    let cacheVal = await getCached(this.cacheKey(record), () => {});
    if (cacheVal) {
      this.recordsUpserted.push(record);
      this.upsertRes.push(cacheVal);
    } else {
      this.recordsToUpsert.push(record);
      if (this.recordsToUpsert.length >= UPSERT_CHUNK_SIZE)
        await this.process();
    }
  }

  cacheKey(destRecord) {
    return upsertCacheKey(this.destConn, destRecord);
  }

  handleResult(destRecord, upsertRes, cacheKey) {
    if (!upsertRes.success || !upsertRes.id) {
      console.error("ERROR SAVING RECORD:", upsertRes.errors);
      clearCache(cacheKey || this.cacheKey(destRecord));
      // process.exit(1);
      throw new Error(
        `ERROR SAVING RECORD ${JSON.stringify(upsertRes.errors)}`
      );
    }
  }

  async process() {
    let bulkRes = await migrateRecords(
      this.destConn,
      this.sobjectName,
      this.recordsToUpsert
    );
    let hasError;
    bulkRes.map((x, idx) => {
      if (!x.success) {
        debug({ errors: x.errors, row: this.recordsToUpsert[idx] });
        hasError = true;
      }
    });

    this.recordsToUpsert = [...this.recordsUpserted, ...bulkRes];
    this.upsertRes = [...this.upsertRes, ...bulkRes];

    this.upsertRes.map((val, i) => {
      this.handleResult(this.recordsToUpsert[i], val);
    });

    this.recordsToUpsert = [];
    if (hasError) throw new Error("upsert errors");
    return this.upsertRes;
  }
}

async function migrateRecords(destConn, sobjectName, records) {
  let fieldNames = new Set();

  // prepare records for upsert to destination system
  records = records.map((record) => {
    // don't mutate the original
    record = { ...record };

    // move id to our custom field
    delete record.attributes;
    if (!record[EXTERNAL_ID_FIELD + "__c"]) {
      record[EXTERNAL_ID_FIELD + "__c"] = record.Id;
      delete record.Id;
    }

    // flatten keys for bulk api
    for (const k of Object.keys(record)) {
      // debug(`flattening ${k} (${record[k]})`);
      if (typeof record[k] === "object") {
        Object.keys(record[k]).map(
          (kk) => (record[k + "." + kk] = record[k][kk])
        );
        delete record[k];
      }
    }

    // don't overwrite nulls/zero/blank/false
    Object.keys(record)
      .filter((f) => !record[f])
      .map((f) => delete record[f]);

    if (!record[EXTERNAL_ID_FIELD + "__c"]) {
      console.error(record);
      throw new Error("missing external id: " + JSON.stringify(record));
    }

    Object.keys(record).map((f) => fieldNames.add(f));

    return record;
  });

  // don't send records already syncd

  fieldNames = [...fieldNames];
  // debug({ fieldNames });
  let idsFilterChunks = chunk(
    records.map((r) => r[EXTERNAL_ID_FIELD + "__c"]).sort(),
    100
  );
  let existingRecordsRes = [];
  for (const idsFilterChunk of idsFilterChunks) {
    let idsFilter = idsFilterChunk.map((i) => `'${i}'`).join(",");
    if (!fieldNames.length) {
      debug(`skipping query because no fields`);
      continue;
    }
    for await (const row of queryAll(
      destConn,
      `select Id,${fieldNames} from ${sobjectName} where ${
        EXTERNAL_ID_FIELD + "__c"
      } in (${idsFilter})`
    ))
      existingRecordsRes.push(row);
  }

  let alreadyDone = records.map((r) => {
    let matched;
    for (const x of existingRecordsRes) {
      if (x[EXTERNAL_ID_FIELD + "__c"] == r[EXTERNAL_ID_FIELD + "__c"]) {
        matched = x;
        break;
      }
    }
    if (!matched) return;
    let diffs = Object.keys(r)
      .filter((f) => Object.keys(matched).includes(f))
      .filter((f) => r[f] != matched[f])
      .filter(
        (f) =>
          ![
            "npo02__LastCloseDate__c",
            "npo02__LastMembershipAmount__c",
            "npo02__LastMembershipDate__c",
            "npo02__NumberOfClosedOpps__c",
            "npo02__NumberOfMembershipOpps__c",
            "npo02__OppAmount2YearsAgo__c",
            "npo02__OppAmountLastNDays__c",
            "npo02__OppAmountLastYear__c",
            "npo02__OppsClosed2YearsAgo__c",
            "npo02__OppsClosedLastNDays__c",
            "npo02__OppsClosedLastYear__c",
            "npo02__TotalMembershipOppAmount__c",
            "npo02__TotalOppAmount__c",
            "npo02__AverageAmount__c",
            "npo02__Best_Gift_Year_Total__c",
            "npo02__Best_Gift_Year__c",
            "npo02__FirstCloseDate__c",
            "npo02__LargestAmount__c",
            "npo02__LastOppAmount__c",
            "npo02__OppAmountThisYear__c",
            "npo02__OppsClosedThisYear__c",
            "npo02__SmallestAmount__c",
          ].includes(f)
      );

    if (diffs.length < 1) {
      return {
        success: true,
        errors: [],
        id: matched.Id,
      };
    }

    debug({
      sourceId: r[EXTERNAL_ID_FIELD + "__c"],
      destId: matched.Id,
      diffs,
      source: r,
      matched,
    });
  });

  // if a single record, skip bulk and do single
  // FIXME this breaks if one record in the table
  if (records.length === 1) {
    if (alreadyDone[0]) return alreadyDone;
    let destRecord = records[0];
    let cacheKey = upsertCacheKey(destConn, destRecord);
    let upsertRes;
    if (sobjectName == "OpportunityContactRole") {
      upsertRes = await getCached(cacheKey, () =>
        destConn
          .sobject(sobjectName)
          .insert([destRecord])
          .then((x) => x[0])
      );
    } else {
      upsertRes = await getCached(cacheKey, () =>
        destConn
          .sobject(sobjectName)
          .upsert([destRecord], EXTERNAL_ID_FIELD + "__c")
          .then((x) => x[0])
      );
    }
    debug({ upsertRes });
    return [upsertRes];
  }

  // send the data

  let res = [];
  let recordsNotAlreadyDone = records.filter(
    (val, slice) => !alreadyDone[slice]
  );
  // debug({ recordsNotAlreadyDone });
  for (const recordsChunk of chunk(recordsNotAlreadyDone, 10_000)) {
    debug("executing bulk upsert...");
    let chunkRes;
    try {
      chunkRes = await destConn
        .sobject(sobjectName)
        .upsertBulk(recordsChunk, EXTERNAL_ID_FIELD + "__c");
    } catch (err) {
      chunkRes = recordsChunk.map(() => ({
        success: false,
        errors: [err],
      }));
    }
    // debug({ chunkRes });
    res = [...res, ...chunkRes];
  }
  // combine the alreadyDone with the updated records
  return alreadyDone.map((x) => x || res.shift());
}

function chunk(listToChunk, chunkSize) {
  let res = [[]];
  listToChunk.map((x) => {
    if (res[res.length - 1].length >= chunkSize) res.push([]);
    res[res.length - 1].push(x);
  });
  return res;
}

async function replicate(
  connectionPrefix,
  destConnectionPrefix,
  objectNameFilter
) {
  debug("processing ", connectionPrefix);
  const x = new ReplicationProcess(
    connectionPrefix,
    destConnectionPrefix,
    objectNameFilter
  );
  await x.initialize();
  // await x.createExternalIds();
  await x.copyObjects();
  debug("Done.");
}

// async function retrieve(conn, sobj, id) {
//   const cacheKey = getCacheKey(conn, path.join("retrieve", sobj, id));
//   return getCached(cacheKey, () => conn.sobject(sobj).retrieve(id));
// }

async function* queryAll(conn, soql) {
  const limit = 5000;
  let records;
  let lastId = "";
  const ID_FIELD = "Id";
  while (!records || records.length > 0) {
    if (lastId) debug(`downloading chunk after ${lastId}`);
    let offsetFilter = lastId
      ? (soql.toLowerCase().includes(" where ") ? " and " : " where ") +
        ` ${ID_FIELD} > '${lastId}' `
      : "";
    let paginatedSoql =
      soql + ` ${offsetFilter} order by ${ID_FIELD} asc limit ${limit} `;
    // debug(paginatedSoql);
    let res = await query(conn, paginatedSoql);
    // debug({ res });
    records = res.records;
    // debug({ records });
    for (const r of records) {
      lastId = r[ID_FIELD];
      yield r;
    }
    if (res.records.length < 1) {
      debug("done downloading");
      // debug({ res });
      break;
    }
  }
}

class ReplicationProcess {
  idMapping = {};

  constructor(connectionPrefix, destConnectionPrefix, objectNameFilter) {
    this.connectionPrefix = connectionPrefix;
    this.destConnectionPrefix = destConnectionPrefix;
    this.objectNameFilter = objectNameFilter;
  }

  /**
   * constructors can't be async, so moving this setup logic here
   */
  async initialize() {
    debug("getting connections...");
    [this.sourceConn, this.destConn] = await Promise.all([
      await salesforce.getConnectionFromStorage(this.connectionPrefix),
      await salesforce.getConnectionFromStorage(this.destConnectionPrefix),
    ]);
    debug("connections set.");
    let ids = (
      await Promise.all(
        [this.sourceConn, this.destConn].map((c) =>
          getCached(getCacheKey(c, "identity"), () => c.identity())
        )
      )
    ).map((x) => x.username);
    debug(`ids: ${ids}`);
    // const sobjects = await this.getObjectsToTransfer();
    // this.cacheTableCounts(sobjects);
  }

  async cacheTableCounts(conn, sobjects) {
    debug("caching counts all tables...");
    await Promise.all(sobjects.map((sobj) => this.getTableCount(conn, sobj)));
  }

  async getTableCount(conn, sobj) {
    const soql = `select count(id) TOTAL_COUNT from ${sobj.name}`;
    return query(conn, soql).catch((e) => debug("ERROR", e));
  }

  async getObjectsToTransfer() {
    let { sobjects } = await describeGlobal(this.sourceConn);
    let { sobjects: sobjectsDest } = await describeGlobal(this.destConn);
    const destObjectNames = sobjectsDest.map((x) => x.name);
    return sobjects
      .filter(
        (sobj) =>
          sobj.queryable &&
          sobj.createable &&
          sobj.updateable &&
          sobj.layoutable &&
          !sobj.deprecatedAndHidden &&
          // only objects in dest org
          destObjectNames.includes(sobj.name) &&
          !BAD_OBJECTS.includes(sobj.name)
      )
      .filter(
        (sobj) =>
          !this.objectNameFilter ||
          sobj.name
            .toLowerCase()
            .trim()
            .startsWith(this.objectNameFilter.toLowerCase().trim())
      );
  }

  async getCount(conn, objectName) {
    if (!conn) throw new Error("conn required");
    const soql = `select count(id) TOTAL_COUNT from ${objectName}`;
    const { TOTAL_COUNT } = (await query(conn, soql)).records[0];
    return TOTAL_COUNT;
  }

  objectsWithExternalIdField = [];

  async createExternalIds() {
    // ensure the external id is on all objects
    debug("adding external ids to destination org...");
    const objs = await this.getObjectsToTransfer();
    let fields = objs.map(async (obj) =>
      ensureExternalIdField(this.destConn, obj.name)
        .then((x) => {
          this.objectsWithExternalIdField.push(obj.name);
          return x;
        })
        .catch(debug)
    );
    fields = await Promise.all(fields);
    fields = fields.filter((f) => !!f);

    debug("adding fields to permission set...");
    await addFieldsToPermissionSet(this.destConn, fields);
  }

  async copyObjects() {
    const objs = await this.getObjectsToTransfer();
    // find the most referenced object and load that first
    // name => count of references
    debug("building refs map...");
    const bar = newBar("ref map generation");
    bar.start(objs.length);
    const objectRefMap = {};
    const _f = async (obj) => {
      debug(`  ${obj.name}`);
      const { fields } = await describeObject(this.sourceConn, obj.name);
      fields
        .filter((f) => f.referenceTo && f.referenceTo.length > 0)
        .map((f) => {
          f.referenceTo.map(
            (f2) => (objectRefMap[f2] = (objectRefMap[f2] || 0) + 1)
          );
        });
      bar.increment();
    };

    await Promise.all(objs.map(_f)).then(debug);
    bar.stop();

    let objectRefs = Object.keys(objectRefMap).map((obj) => ({
      obj,
      count: objectRefMap[obj],
    }));
    objectRefs.sort((a, b) => b.count - a.count);
    debug({ objectRefs });

    let allObjs = await this.getObjectsToTransfer();

    // first load the records without lookups that have the most dependencies

    // for (const { obj: objName } of objectRefs) {
    //   let obj = allObjs.filter((o) => o.name == objName)[0];
    //   if (!obj) {
    //     debug("obj not found: ", objName);
    //     continue;
    //   }

    //   let refDesc = await describeObject(this.destConn, obj.name);
    //   if (
    //     refDesc.fields.filter((f) => f.name == EXTERNAL_ID_FIELD + "__c")
    //       .length > 0
    //   ) {
    //     // uncomment this when doing a first load into an environmnet
    //     // await this.copyObject2(obj, undefined, { doRecursive: false });

    //     await this.copyObject2(obj, undefined, { doRecursive: false });
    //   }
    // }
    // then load them all with lookups
    for (const obj of allObjs) {
      try {
        await this.copyObject2(obj, undefined, { doRecursive: true });
      } catch (e) {
        console.error({ e });
      }
    }
  }

  idsInFlight = [];

  async loadIdMapping(sobj) {
    if (this.idMapping["loaded-" + sobj.name])
      return debug("loadIdMapping already completed:", sobj.name);
    debug("loadIdMapping starting:", sobj.name);
    let { fields } = await describeObject(this.destConn, sobj.name);
    fields = fields.filter((f) => f.name == EXTERNAL_ID_FIELD + "__c");
    if (!fields.length) return;
    let soql = `select Id, ${EXTERNAL_ID_FIELD}__c from ${sobj.name} where ${EXTERNAL_ID_FIELD}__c != '' and IsDeleted = false `;
    for await (const row of queryAll(this.destConn, soql)) {
      this.idMapping[row[`${EXTERNAL_ID_FIELD}__c`]] = row.Id;
    }
    this.idMapping["loaded-" + sobj.name] = 1;
  }

  async copyObject2(sobj, id, opts) {
    // don't insert users
    if (sobj.name == "User") return;

    const { doRecursive } = opts || {};
    debug(`copyObject2 starting: ${sobj.name}, ${id}`);
    await this.loadIdMapping(sobj);
    let { fields } = await describeObject(this.sourceConn, sobj.name);
    let { fields: destFields } = await describeObject(this.destConn, sobj.name);
    // debug({ fields });
    let soqlFields = fields.map((x) => x.name).join(",");
    if (fields.length < 1) {
      return debug("skipping query because no fields to query:", sobj.name);
    }
    let soql = `select ${soqlFields} from ${sobj.name}`;

    const bar = newBar(sobj.name);

    if (id) {
      if (this.idsInFlight.includes(id)) {
        debug(`RECURSIVE: ${id}`);
        return;
      }
      this.idsInFlight.push(id);
      soql += ` where Id = '${id}' `;
    } else {
      const {
        records: [{ TOTAL_COUNT }],
      } = await query(
        this.sourceConn,
        `select count(id) TOTAL_COUNT from ${sobj.name}`
      );
      bar.start(TOTAL_COUNT);
    }
    let upsertManager = new UpsertManager(this.destConn, sobj.name);
    for await (const record of queryAll(this.sourceConn, soql)) {
      bar.increment(1, { id: record.Id });

      // prune empty fields
      Object.keys(record)
        .filter((k) => !record[k])
        .map((k) => delete record[k]);

      // debug({ record });

      let destRecord = {};
      destRecord[EXTERNAL_ID_FIELD + "__c"] = record.Id;
      for (const f of destFields) {
        if (f.name == "RecordTypeId") continue; // TODO
        if (sobj.name == "Opportunity" && f.name == "ContactId") continue; // WTF
        // FIXME: how to handle converted leads?  we don't want to recreate them probably, unless later we would convert them?
        // if (sobj.name == "Lead" && f.name == "IsConverted") continue;
        if (!f.createable) continue;
        // if (!f.updateable) continue;
        const val = record[f.name];
        if (!val) continue;

        if (f.type == "reference") {
          // even if we don't do recursive, if the mapped object was loaded, we can get the lookup ids
          if (this.idMapping[val]) {
            debug(`  using existing id ${val} => ${this.idMapping[val]}`);
            destRecord[f.name] = this.idMapping[val];
            continue;
          }

          if (f.referenceTo && f.referenceTo.length > 2) {
            console.error("too many references:", f.referenceTo);
            process.exit(1);
          }

          // don't do lookups to objects with no external id field
          let referenceToCleaned = [];
          for (const x of f.referenceTo) {
            // debug({ x });
            const refDesc = await describeObject(this.destConn, x);
            const refFields = refDesc.fields.map((f) => f.name).sort();
            // debug({ refFields });
            if (refFields.includes(EXTERNAL_ID_FIELD + "__c")) {
              referenceToCleaned.push(x);
              await this.loadIdMapping(refDesc);
            }
          }

          // try again after making mappings
          if (!referenceToCleaned || referenceToCleaned.length === 0) {
            debug(`  no target objects for field: ${f.name}`);
            // debug({ f });

            continue;
          }

          // if (f.referenceTo[0] == "User") continue; // TODO

          // TODO loop through mappings

          try {
            let { records: targetRecords } = await query(
              this.destConn,
              `select Id from ${f.referenceTo[0]} where ${EXTERNAL_ID_FIELD}__c = '${val}' and IsDeleted = false `
            );
            if (targetRecords.length == 1) {
              debug(`[${record.Id}] value found: ${targetRecords[0].Id}`);
              destRecord[f.name] = targetRecords[0].Id;
            }

            // else {
            //   debug(`[${record.Id}] MISSING LOOKUP: ${f.name} = ${val}`);
            //   debug({ targetRecords });
            //   let refObj = await describeObject(
            //     this.sourceConn,
            //     f.referenceTo[0]
            //   );
            //   // skip ref population if it doesn't have our id field
            //   let refDesc = await describeObject(this.destConn, refObj.name);
            //   if (
            //     refDesc.fields.filter(
            //       (f) => f.name == EXTERNAL_ID_FIELD + "__c"
            //     ).length > 0
            //   ) {
            //     let refRes = await this.copyObject2(refObj, val);
            //     debug({ refRes });
            //     if (!refRes || !refRes[0].success) {
            //       console.error(refRes[0]);
            //       continue;
            //     }
            //     destRecord[f.name] = refRes[0].id;
            //   }
            // }
          } catch (e) {
            debug(`[${record.Id}] ERROR GETTING TARGET:`);
            debug(e);
            continue;
          }
        } else {
          destRecord[f.name] = val;
        }
      }

      // debug({ type: sobj.name, destRecord });
      if (
        sobj.name == "OpportunityContactRole" &&
        (!destRecord.ContactId || !destRecord.OpportunityId)
      )
        continue;
      await upsertManager.addRecord(destRecord);
    }
    let upsertRes = await upsertManager.process();
    // res.map(x => this.idMapping[record.id] = upsertRes.id)

    if (id) this.idsInFlight = this.idsInFlight.filter((i) => i != id);
    return upsertRes;
  }
}

module.exports = replicate;
