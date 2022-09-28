const puppeteer = require("puppeteer");
const salesforce = require("./salesforce");
const storage = require("./storage");
const path = require("path");
const fs = require("fs");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function download(page, f) {
  const downloadPath = path.resolve(
    process.cwd(),
    `temp/download-${Math.random().toString(36).substr(2, 8)}`
  );
  fs.mkdirSync(downloadPath, { recursive: true });
  console.log("Download directory:", downloadPath);
  await page._client.send("Page.setDownloadBehavior", {
    behavior: "allow",
    downloadPath: downloadPath,
  });
  await f();
  console.error("Downloading...");
  let fileName;
  const startedWaiting = new Date();
  while (!fileName || fileName.endsWith(".crdownload")) {
    if (new Date().getTime() - startedWaiting.getTime() > 5 * 60 * 1000)
      throw new Error("download of zip took too long");
    await new Promise((resolve) => setTimeout(resolve, 1000));
    [fileName] = fs.readdirSync(downloadPath);
  }
  const filePath = path.resolve(downloadPath, fileName);
  console.error("Downloaded file:", filePath);
  return filePath;
}

module.export = async (connectionPrefix) => {
  const account = connectionPrefix;
  const auths = await storage.list(path.join(account, "/__auth/"));
  const authKey = auths[auths.length - 1].Key;
  const conn = await salesforce.getConnectionFromStorage(authKey);

  // execute one query to ensure the oauth is refreshed and ready
  console.log("limits:", await conn.limits());

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox"],
  });

  const page = await browser.newPage();

  // https://help.salesforce.com/articleView?id=000332032&type=1&mode=1
  const loginUrl = path.join(
    conn.instanceUrl,
    "/secur/frontdoor.jsp?sid=" +
      conn.accessToken +
      "&retURL=/ui/setup/export/DataExportPage/d"
  );
  await page.goto(loginUrl, { waitUntil: "networkidle2" });

  const downloadLinks = await page.$$eval("a.actionLink", (links) =>
    links.map((link) => [link.href, link.textContent])
  );
  console.log("downloadLinks: ", downloadLinks);

  for (var downloadLink of downloadLinks) {
    console.log({ downloadLink });
    downloadLink = downloadLink[0];

    const storageKey = path.join(
      account,
      "__zips_v2",
      downloadLink.replace(/[^a-zA-Z0-9]+/g, "_")
    );

    // skip if already downloaded and a good size
    if (await storage.exists(storageKey)) {
      console.log("  file already archived, skipping");
      continue;
    }

    var downloadLinkEnding =
      downloadLink.split("/")[downloadLink.split("/").length - 1];

    console.log("clicking link...", downloadLinkEnding);

    const downloadedFile = await download(page, () =>
      page.click(`a[href*='${downloadLinkEnding}']`)
    );

    console.log("DONE!!!!", { downloadedFile });

    // todo upload to s3

    console.log("Uploading to storage...");
    const readStream = fs.createReadStream(downloadedFile);
    await storage.set(storageKey, readStream);
    readStream.destroy();
    console.log("Done!");

    // todo delete local file to save space
    console.log("Deleting file...");
    fs.unlinkSync(downloadedFile);
    console.log("Done!");

    // break;
  }
  await browser.close();
};
