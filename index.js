const debug = require("debug")("index");
const express = require("express");
const helmet = require("helmet");
const jsforce = require("jsforce");
const proxy = require("express-http-proxy");
const path = require("path");
const cookieSession = require("cookie-session");
const settings = require("./settings");
const storage = require("./storage");
const salesforce = require("./salesforce");
const fetch = require("node-fetch");
const api = require("./routes/api");

const app = express();
app.use(helmet());
app.set("view engine", "pug");
app.set("trust proxy", 1); // trust first proxy which is heroku
app.use("/static", express.static("public"));
app.use(
  cookieSession({
    // name: "session",
    keys: [settings.SESSION_SECRET || "1111111_CHANGE_ME_1111111"],
    // maxAge: 24 * 60 * 60 * 1000, // 24 hours
    // *** comment out the next two lines if running localhost ***
    // sameSite: "none",
    // secure: true,
    httpOnly: true,
  })
);

app.post(
  "/salesforce_outbound_message_webhook",
  express.text({ type: "*/*", limit: "10mb" }),
  async (req, res) => {
    const key = "salesforce_outbound_messages/" + new Date().toISOString();
    debug({ key, body: req.body, headers: req.headers });
    if (!req.body) return res.status(400).send("no body");
    await storage.set(key, req.body);
    res.send(
      `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
        <soapenv:Body>
            <notificationsResponse xmlns="http://soap.sforce.com/2005/09/outbound">
                <Ack>true</Ack>
            </notificationsResponse>
        </soapenv:Body>
    </soapenv:Envelope>`
    );
  }
);

app.get("/auth", function (req, res) {
  req.session.retURL = req.query.retURL;
  const loginUrl = req.query.loginUrl || "https://login.salesforce.com";
  var oauth2 = new jsforce.OAuth2({
    clientId: settings.SALESFORCE_CLIENT_ID,
    clientSecret: settings.SALESFORCE_CLIENT_SECRET,
    redirectUri: settings.SALESFORCE_CALLBACK_URL,
    loginUrl,
  });
  res.redirect(oauth2.getAuthorizationUrl({ prompt: "consent" }));
});

app.get("/auth_sandbox", function (req, res) {
  let loginUrl = "https://test.salesforce.com";
  req.session = { loginUrl };
  var oauth2 = new jsforce.OAuth2({
    loginUrl,
    clientId: settings.SALESFORCE_CLIENT_ID,
    clientSecret: settings.SALESFORCE_CLIENT_SECRET,
    redirectUri: settings.SALESFORCE_CALLBACK_URL,
  });
  res.redirect(oauth2.getAuthorizationUrl({ prompt: "consent" }));
});

app.get(
  ["/oauth2/callback", "/auth/callback", "/callback"],
  async (req, res) => {
    let data = {
      grant_type: "authorization_code",
      code: req.query["code"],
      client_id: settings.SALESFORCE_CLIENT_ID,
      client_secret: settings.SALESFORCE_CLIENT_SECRET,
      redirect_uri: settings.SALESFORCE_CALLBACK_URL,
      format: "json",
    };
    if (!data.code) {
      return res.send("Error: missing code");
    }

    let { loginUrl } = req.session;
    if (!loginUrl) loginUrl = "https://login.salesforce.com";
    let url = loginUrl + "/services/oauth2/token";

    let x = await fetch(url, {
      method: "POST",
      body: new URLSearchParams(data),
    })
      .then((x) => x.json())
      .catch((x) => x);

    if (!x.id) {
      console.error("no id returned:", x);
      return res.status(400).send(`no id returned <a href="/auth">retry</a>`);
    }

    let connectionId = x.id.split(".com/id/")[1];
    console.log({ connectionId });

    // save to storage
    const ts = new Date().toISOString();
    await storage.put(path.join("salesforce", connectionId, "__auth", ts), x);

    // give user a cookie
    req.session["loginDate"] = new Date().getTime();
    req.session["id"] = connectionId;

    if (req.session["retURL"]) {
      res.redirect(req.session["retURL"]);
      req.session.retURL = undefined; // clear after successful redirect
    } else {
      const dashboardUrl = path.join("/dashboard/salesforce/", connectionId);
      res.redirect(301, dashboardUrl);
    }
  }
);

app.get("/logout", (req, res) => {
  req.session = null;
  if (req.query.retURL) res.redirect(req.query.retURL);
  else res.redirect("/");
});

app.get("/dashboard/*", async (req, res) => {
  let prefix = req.params[0];
  let connectionId = req.params[0];
  let connectionIdParts = connectionId.split("/");
  if (connectionIdParts.length < 3) return res.send("error: bad connection");
  connectionId = connectionIdParts.slice(0, 3).join("/");
  // we expect connection folders to end in slash
  if (connectionId && !connectionId.endsWith("/")) connectionId += "/";
  if (!connectionId) return res.status(404).send("no connection");
  if (!req.session?.id) return res.redirect("/");
  if (
    req.session.id &&
    req.session.id.length >= 15 &&
    !connectionId.includes(req.session.id)
  )
    return res.status(401).send("unauthorized");
  let id;
  let error;
  for await (const k of storage.list(path.join(connectionId, "__id/"))) {
    id = JSON.parse(await storage.get(k));
    break;
  }
  if (!id) {
    try {
      let conn = await salesforce.getConnectionFromStorage(connectionId);
      id = await conn.identity();
      await storage.put(
        path.join(connectionId, "__id", new Date().toISOString()),
        id
      );
    } catch (e) {
      console.error(e);
      error = e;
    }
  }
  let folders = [];
  for await (const f of storage.list(prefix)) folders.push(f);
  res.render("dashboard", { connectionId, id, error, folders });
});

app.get("/", (req, res) => res.render("index"));

// plausible analytics
app.use(
  proxy("https://plausible.io", {
    filter: function (req, res) {
      console.log(req.method, req.path);
      if (req.path == "/js/script.js") return true;
      if (req.path == "/js/plausible.js") return true;
      if (req.path == "/api/event") return true;
    },
  })
);

app.use("/api", api);

const PORT = settings.PORT || 3000;

app.listen(PORT, () => console.log("node server listening on port", PORT));
