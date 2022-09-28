const salesforce = require("../salesforce");
const storage = require("../storage");
const path = require("path");
const axios = require("axios");
const uuid = require("uuid");
const express = require("express");
const cors = require("cors");
const router = express.Router();
const settings = require("../settings");

router.use(
  cors({
    credentials: true,
    // TODO convert to some kind of glob or env var
    origin: function (origin, callback) {
      const allowed =
        !origin ||
        origin.startsWith("http://localhost:") ||
        origin.includes("dbsaver.com");
      if (allowed) {
        return callback(null, true);
      } else {
        callback(new Error("Now allowed"));
      }
    },
    methods: "GET,POST,DELETE,PUT,OPTIONS",
  })
);

router.use(express.json());

async function proxycheck(req) {
  if (!settings.PROXYCHECK_API_KEY) {
    console.warn("missing PROXYCHECK_API_KEY");
    return {};
  }
  const ip = req.headers["x-forwarded-for"] || req.connection.remoteAddress;
  const API_KEY = settings.PROXYCHECK_API_KEY;
  const url = `http://proxycheck.io/v2/${ip}?key=${API_KEY}&vpn=1&asn=1&risk=1&port=1&seen=1&days=30`;
  const data = await axios.get(url).then((res) => res.data);
  console.log("prodycheck data:", data);
  return data[ip];
}

/**
 * redirects the user if no auth is established
 */
router.get("/auth/redirect", async (req, res) => {
  // check if risky ip address before completing auth
  let x = await proxycheck(req);
  if (x.risk && x.risk > 15)
    return res
      .status(400)
      .send(
        "WARNING: You are coming from an untrusted network.  Please use a reputable VPN or turn off all proxy servers."
      );

  const returnURL = req.body.returnURL || req.query.returnURL;
  if (!returnURL) {
    return res.status(400).json({ error: "no returnURL provided" });
  }
  // save returnurl for when the user comes back
  req.session = { start: new Date().getTime() };
  req.session.retURL = returnURL;
  const base64data = Buffer.from(returnURL).toString("base64");
  res.redirect(
    salesforce
      .oauth2()
      .getAuthorizationUrl({ state: base64data, prompt: "consent" })
  );
});

router.get("/auth", (req, res) => {
  if (req.session.id) {
    if (!req.session.user_id) {
      req.session.user_id = req.session.id.split("/")[1];
    }
    return res.json(req.session);
  } else {
    return res.status(400).json({ error: "no auth" });
  }
});

router.get("/salesforce/:connectionId(*)", (req, res) => {
  res.json({ connections: [], backups: [] });
});

router.get(["/data/", "/data/:prefix([^/]*)"], async (req, res) => {
  if (!req.session?.id) {
    return res.status(400).json({ error: "not authenticated" });
  }
  const prefix = req.params.prefix || "";
  const prefixParts = prefix.split("/");
  const prefixPartsUnderscore = prefixParts.filter((p) => p.startsWith("_"));
  if (prefixPartsUnderscore.length > 0 && prefix != "__weeklyzips/") {
    return res.json({ prefix, error: "disallowed url" });
  }
  const userPrefix = path.join("salesforce", req.session.id);
  let fullPrefix = path.join(userPrefix, prefix);
  if (!fullPrefix.endsWith("/")) fullPrefix += "/";
  console.log({ fullPrefix });
  let data = [];
  for await (const k of storage.listFiles(fullPrefix)) {
    k.Key = k.Key || k.Prefix;
    if (path.basename(k.Key).startsWith("_")) continue; // exclude hidden files
    // make relative so client doesn't need to know full paths
    k.Key = path.relative(userPrefix, k.Key);
    data.push(k);
  }

  // todo real chart data
  const labels = ["January", "February", "March", "April", "May", "June"];
  const chart = {
    labels,
    data: {
      labels: labels,
      datasets: [
        {
          label: "My First dataset",
          backgroundColor: "rgb(255, 99, 132)",
          borderColor: "rgb(255, 99, 132)",
          data: [0, 10, 5, 2, 20, 30, 45],
        },
      ],
    },
  };

  res.json({ charts: [chart], prefix, data });
});

// TODO rate limit
router.post("/data/:prefix([^/]*)", async (req, res) => {
  if (!req.session?.id) {
    return res.status(400).json({ error: "not authenticated" });
  }
  const { prefix } = req.params;
  if (!prefix) {
    return res.status(400).json({ error: "missing path prefix" });
  }
  // TODO: better whitelist
  console.log({ prefix });
  if (prefix.startsWith("__weeklyzips/")) {
    return res.json({ prefix, error: "disallowed url" });
  }
  const userPrefix = path.join("salesforce", req.session.id);
  const fullPrefix = path.join(userPrefix, prefix);
  let data = [];
  for await (const k of storage.listFiles(fullPrefix)) {
    k.Key = path.relative(userPrefix, k.Key);
    data.push(k);
  }
  if (res.length !== 1)
    return res.status(400).json({ error: "file match error" });
  // TODO enqueue download
  res.json({ status: "ok", id: uuid.v4() });
});
module.exports = router;
