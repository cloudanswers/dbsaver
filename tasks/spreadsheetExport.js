const cliProgress = require("cli-progress");
const Bottleneck = require("bottleneck/es5");
const debug = require("debug")("tasks");
const fs = require("fs");
const path = require("path");
const salesforce = require("../salesforce");
const storage = require("../storage");
const XLSX = require("xlsx");
const { getCached, getCacheKey, clearCache } = require("./cache");
const sleep = (ms) => new Promise((_) => setTimeout(_, ms));
const limiter = new Bottleneck({
  maxConcurrent: 5,
});

const MAX_RECORDS_TO_DOWNLOAD = 500_000;

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
  // "EntityParticle",
  "FeedAttachment",
  // "FeedComment",
  // "FeedItem",
  "FeedRevision",
  "FieldDefinition",
  "FormulaFunction",
  "ApexTestResult",
  "LightningUsageByAppTypeMetrics",
  "LightningUsageByPageMetrics",

  // SKIPPING FOR NOW
  // "CaseFeed",
  // "CaseHistory",
  "CaseShare",
  // "EmailMessage",
  // "EmailMessageRelation",
  "FieldPermissions",
  "DuplicateRecordItem",
  // "CampaignMember",
];

let multibar;

function bar(name, size) {
  if (!multibar) {
    multibar = new cliProgress.MultiBar(
      {
        clearOnComplete: true,
        stopOnComplete: false,
        format:
          name + " [{name}] {percentage}% | ETA: {eta}s | {value}/{total}",
      },
      cliProgress.Presets.shades_grey
    );
  }
  let bar = multibar.create({});
  bar.start(size, 0, { name });
  bar._cleanup = () => {
    // bar.close();
    multibar.remove(bar);
  };
  return bar;
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

async function spreadsheetExport(connectionPrefix, objectFilter) {
  // debug("processing ", connectionPrefix);
  const conn = await getConnection(connectionPrefix);

  const outputFolder = "output/" + connectionPrefix + new Date().getTime();
  fs.mkdirSync(outputFolder, { recursive: true });

  let { sobjects } = await getCached(getCacheKey(conn, "describeGlobal"), () =>
    conn.describeGlobal()
  );

  // console.log(conn.limitInfo);

  let fieldsData = [];
  let summaryData = [];

  sobjects = sobjects.filter(
    (sobj) =>
      sobj.queryable &&
      // sobj.createable &&
      // sobj.layoutable &&
      (!objectFilter ||
        sobj.name.toLowerCase().trim() == objectFilter.toLowerCase().trim()) &&
      !BAD_OBJECTS.includes(sobj.name)
  );

  const countBar = bar("counts", sobjects.length);
  await Promise.all(
    sobjects.map((sobj) => {
      const soql = `select count(id) TOTAL_COUNT from ${sobj.name}`;

      getCached(getCacheKey(conn, soql), () => conn.queryAll(soql)).catch(
        () => {
          // TODO ignore "field id does not support aggregate operator COUNT"
        }
      );
      countBar.increment();
    })
  );
  countBar._cleanup();

  const load = async (sobj) => {
    // debug(`processing ${sobj.name}`);
    let k = sobj.name;
    // debug(sobj);

    // figure out if there is data to process

    const soql = `select count(id) TOTAL_COUNT from ${sobj.name} where CommentCount > 0`;
    const { TOTAL_COUNT } = (
      await getCached(getCacheKey(conn, soql), () => conn.queryAll(soql)).catch(
        (e) => {
          // debug(`skipping empty or no access: ${sobj.name} ${e}`);
          return { records: [{}] };
        }
      )
    ).records[0];
    if (!TOTAL_COUNT) {
      // debug(`[${sobj.name}] skipping empty table`);
      return;
    }

    let { fields } = await getCached(
      getCacheKey(conn, sobj.name, "describe"),
      () => conn.describe(sobj.name)
    );

    let data = [];
    let whereFilter = "";
    let soqlLimit = 5; // start at 200 because of fields(all)
    let soqlFields =
      // "fields(all)"
      "Id, (select Id, CommentBody,CommentType,FeedItemId,HasEntityLinks,InsertedById,IsRichText,IsVerified,LastEditById,LastEditDate,ParentId,RelatedRecordId,Revision,Status,ThreadChildrenCount,ThreadLastUpdatedDate,ThreadLevel,ThreadParentId from FeedComments)";

    const bar0 = bar(sobj.name + " queries", TOTAL_COUNT);

    while (true) {
      // debug(`  querying ${k}`);
      let soql = `select ${soqlFields} from ${k} where CommentCount > 0 ${whereFilter} order by Id asc limit ${soqlLimit}`;
      // debug({ soql, whereFilter });
      let chunk;
      try {
        const cacheKey = getCacheKey(conn, soql);
        chunk = await getCached(cacheKey, () => conn.queryAll(soql));
        if (chunk.errors?.length) {
          clearCache(cacheKey);
        }
      } catch (e) {
        console.error({ e, soql });
        // data = [{ error: JSON.stringify(e) }];
        continue;
      }
      data = [...data, ...chunk.records];
      bar0.update(data.length);
      // let statusMessage = `  chunk downloaded ${data.length} oldest:${
      //   data[data.length - 1]?.CreatedDate
      // }`;
      // debug(statusMessage);

      // can't use soqlLimit value because sometimes we don't get a full load
      if (chunk.records.length < 1) break;

      // soqlFields = Object.keys(chunk.records[0])
      //   .filter((x) => x != "attributes")
      //   .join(",");
      // soqlLimit = 2000;

      whereFilter = ` and Id > '${data[data.length - 1].Id}' `;

      // break; // debug, only show one chunk
      if (data.length >= MAX_RECORDS_TO_DOWNLOAD) {
        debug(`[${sobj.name}] HIT DOWNLOAD LIMIT, ENDING EARLY`);
        break;
      }
    }

    bar0._cleanup();

    // console.log("data.length", data.length);

    // write worksheet if it has data
    if (data.length) {
      const MAX_LENGTH = 32767; // 32767
      // workaround for UnhandledPromiseRejectionWarning: Error: Text length must not exceed 32767 characters
      const trimBar = bar(`${sobj.name} trims`, data.length);
      data.map((row) => {
        Object.keys(row).map((f) => {
          if (`${row[f]}`.length > MAX_LENGTH)
            row[f] = row[f].substring(0, MAX_LENGTH);
        });
        trimBar.increment();
      });
      trimBar._cleanup();

      // excel has 31 character worksheet name limit
      let wsName = k.length > 31 ? k.substring(0, 31) : k;
      const wb = XLSX.utils.book_new();

      // summarize fields
      // debug(`[${sobj.name}] sumarizing fields...`);
      const sumBar = bar(`summarize fields`, fields.length);
      let fieldSummary = fields.map((f) => {
        let valueCount = data
          .map((row) => new String(row[f.name] || "").trim().substring(0, 255))
          .reduce((prev, curr) => {
            if (curr == null) curr = "";
            curr = `${curr}`.trim();
            prev[curr] = (prev[curr] || 0) + 1;
            return prev;
          }, {});
        // console.log({ valueCount });
        let valueList = Object.keys(valueCount).map((k) => ({
          name: k,
          count: valueCount[k],
        }));
        // sort by value
        valueList.sort(function (a, b) {
          return b.count - a.count;
        });
        let res = {
          name: f.name,
          label: f.label,
          BLANKS: valueCount[""],
          UNIQUE_VALUES: valueList.length,
        };
        valueList = valueList.filter((x) => x.name != "");
        for (let i = 0; i < 5; i++) {
          const x = valueList[i] || {};
          res[`VAL_${i + 1}`] = x.name;
          res[`VAL_${i + 1}_COUNT`] = x.count;
        }
        sumBar.increment();
        return res;
      });

      sumBar._cleanup();

      debug(`[${sobj.name}] writing field summary to worksheet...`);
      XLSX.utils.book_append_sheet(
        wb,
        XLSX.utils.json_to_sheet(fieldSummary),
        "FIELD_SUMMARY"
      );

      debug(`[${sobj.name}] appending worksheet to workbook...`);
      XLSX.utils.book_append_sheet(
        wb,
        XLSX.utils.json_to_sheet(
          fields.map((f) => {
            // put name and label at start
            return { name: f.name, label: f.label, ...f };
          })
        ),
        "FIELDS"
      );

      // output full data
      const MAX_ROWS = 25000;

      // ************** TEMP combine subquery ****************
      // console.log(data);
      data = data
        .map((x) => x.FeedComments.records)
        .filter((x) => !!x)
        .reduce((prev, curr) => [...prev, ...curr], []);

      for (let i = 0; i < data.length; i = i + MAX_ROWS) {
        if (i >= data.length) continue;
        const ws = XLSX.utils.json_to_sheet(data.slice(i, i + MAX_ROWS));
        debug(`[${sobj.name}] adding full data to workbook...`);
        XLSX.utils.book_append_sheet(wb, ws, wsName + (i > 0 ? ` ${i}` : ""));
      }

      debug(`[${sobj.name}] saving workbook to disk...`);
      XLSX.writeFile(wb, path.join(outputFolder, `${k}.xlsx`));
      debug(`[${sobj.name}] done.`);

      summaryData.push({ sobject: sobj.name, records: data.length });

      fieldsData = [
        ...fieldsData,
        ...fieldSummary.map((f) => ({ sobject: sobj.name, ...f })),
      ];
    }
  };

  let promises = sobjects.map((sobj) => limiter.schedule(() => load(sobj)));

  await Promise.all(promises);

  debug("done processing objects, creating summary workbook...");

  const wb = XLSX.utils.book_new();

  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(fieldsData),
    "FIELDS"
  );
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(summaryData),
    "SUMMARY"
  );

  const outFilename =
    connectionPrefix.replace(/\//g, "_") + new Date().getTime() + ".xlsx";
  console.log({ outFilename });
  XLSX.writeFile(wb, path.join(outputFolder, outFilename));
  console.log("Done.");
}

module.exports = spreadsheetExport;
