const salesforce = require("./salesforce");
const jsforce = require("jsforce");
const storage = require("./storage");
const { STREAM_DOWNLOAD_CONCURRENCY_LIMIT } = require("./settings");
const path = require("path");
const Bottleneck = require("bottleneck/es5");

async function stream(prefix) {
  const pathPrefix = path.join(prefix, "__ChangeDataCapture/");
  const conn = await salesforce.getConnectionFromStorage(prefix);

  // TODO check if change data capture is configured?
  // must do one other api call before streaming works, not sure why:
  // await conn.query("select id from account limit 1").then(console.log);
  // await conn.identity().then(console.log);
  console.log(prefix, "checking limits...");
  try {
    console.log(prefix, await conn.limits());
  } catch (e) {
    console.error(prefix, "error getting limits:", e);
  }

  // The Salesforce streaming topic and position to start from.
  const channel = "/data/ChangeEvents";

  // https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm
  // -2 = all available
  let replayId = -2;

  for await (const key of storage.list(pathPrefix)) {
    console.log(prefix, { key });
    // TODO could we just parse the key instead of getting the object
    let data = await storage.getJson(key);
    // console.log({ data });
    replayId = data.event.replayId || replayId;
    break;
  }
  // console.log({ replayId });
  // process.exit();
  // -1 means no replay, start from now
  // 0 means what?
  // TODO find last replayId which we can use?
  // https://developer.salesforce.com/docs/atlas.en-us.236.0.api_streaming.meta/api_streaming/using_streaming_api_durability.htm

  // Construct a streaming client.
  const streamClient = conn.streaming.createClient([
    new jsforce.StreamingExtension.Replay(channel, replayId),
    new jsforce.StreamingExtension.AuthFailure((err) => {
      console.error(prefix, "StreamingExtension.AuthFailure:", err);
      throw new Error(prefix + "StreamingExtension.AuthFailure:", err);
    }),
  ]);
  let lastEventTime = new Date().getTime();
  const subscription = streamClient.subscribe(channel, (data) => {
    console.log("topic received data", JSON.stringify(data));
    if (!data?.event?.replayId) {
      throw new Error(prefix + "missing replayId on data object");
    }

    // a reversed key enables us to query only the top record in a folder to know the latest record
    // since S3 won't let us sort keys during listing
    const reversedId = `${1_000_000_000_000 - data.event.replayId}`;

    const outFile = path.join(
      prefix,
      "__ChangeDataCapture/",
      `${reversedId}__${data.event.replayId}`
    );

    console.log(prefix, { outFile, reversedId });
    storage.put(outFile, data);
    lastEventTime = new Date().getTime();
  });

  // cancel if no events in 30 seconds
  const intervalCancel = setInterval(() => {
    if (lastEventTime < new Date().getTime() - 30_000) {
      console.log(
        prefix,
        "cancelling streaming subscription due to no new events"
      );
      subscription.cancel();
      if (intervalCancel) clearInterval(intervalCancel);
    }
  }, 10_000);
}

async function main(prefix = "salesforce/") {
  let seen = [];
  let promises = [];
  for await (const orgPrefix of storage.list(prefix)) {
    // if (orgPrefix.split("/").length != 2) continue;
    console.log({ orgPrefix });
    for await (const userPrefix of storage.list(orgPrefix)) {
      if (seen.includes(userPrefix)) continue;
      seen.push(userPrefix);
      if (userPrefix.split("/").filter((x) => x).length != 3) continue;
      console.log({ userPrefix });
      promises.push(stream(userPrefix).catch(console.error));
    }
  }
  await Promise.all(promises);
}

if (require.main === module) {
  main()
    .then(() => console.log("all streams done"))
    .catch(console.error);
}
