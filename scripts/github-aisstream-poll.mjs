const AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream";
const RUN_MS = 150_000; // 2 min 30 s
const FLUSH_INTERVAL_MS = 2_000;
const BATCH_SIZE = 100;
const BBOX = [[-6.0, -20.0], [20.0, 13.0]];
const PORTS_BBOX = {
  abidjan: { south: 4.80, west: -4.80, north: 5.80, east: -3.20 },
  "pointe-noire": { south: -5.5, west: 11.2, north: -4.2, east: 12.5 },
};

const env = {
  aisKey: process.env.AISSTREAM_API_KEY ?? "",
  supabaseUrl: process.env.SUPABASE_URL ?? "",
  serviceKey: process.env.SUPABASE_SERVICE_ROLE_KEY ?? "",
};
const missing = [!env.aisKey && "AISSTREAM_API_KEY", !env.supabaseUrl && "SUPABASE_URL", !env.serviceKey && "SUPABASE_SERVICE_ROLE_KEY"].filter(Boolean);
if (missing.length) throw new Error(`Secrets manquants: ${missing.join(", ")}`);

const queue = [];
const staticByMmsi = new Map();
const slowSinceByMmsi = new Map();
let received = 0;
let rejected = 0;
let inserted = 0;
const rejectedByReason = {};

const reject = (reason) => { rejected += 1; rejectedByReason[reason] = (rejectedByReason[reason] ?? 0) + 1; };
const text = (value) => typeof value === "string" ? value.trim() : "";
const validPosition = (lat, lon) => Number.isFinite(lat) && Number.isFinite(lon) && lat !== 0 && lon !== 0 && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
const firstCoordinate = (...values) => values.map(Number).find((value) => Number.isFinite(value) && value !== 0) ?? 0;
const inferPort = (lat, lon) => Object.entries(PORTS_BBOX).find(([, box]) => lat >= box.south && lat <= box.north && lon >= box.west && lon <= box.east)?.[0] ?? "offshore";
const supabaseUrl = (resource) => `${env.supabaseUrl.replace(/\/$/, "")}/rest/v1/${resource}`;

async function insert(resource, rows) {
  if (!rows.length) return;
  const response = await fetch(supabaseUrl(resource), { method: "POST", headers: { apikey: env.serviceKey, Authorization: `Bearer ${env.serviceKey}`, "Content-Type": "application/json", Prefer: "resolution=merge-duplicates,return=minimal" }, body: JSON.stringify(rows) });
  if (!response.ok) throw new Error(`Supabase ${resource} ${response.status}: ${await response.text()}`);
}

async function flush() {
  if (!queue.length) return;
  const rows = queue.splice(0, BATCH_SIZE);
  try {
    await insert("ais_positions", rows);
    inserted += rows.length;
    console.log(JSON.stringify({ event: "supabase_insert_success", positions: rows.length, inserted }));
  } catch (error) {
    queue.unshift(...rows);
    console.error(JSON.stringify({ event: "supabase_insert_error", error: String(error) }));
  }
}

async function evaluateCriticalAlert({ mmsi, name, port, lat, lon, sog, cog }) {
  if (port === "offshore" || sog == null || sog > 0.5) { slowSinceByMmsi.delete(mmsi); return; }
  const now = Date.now();
  const slowSince = slowSinceByMmsi.get(mmsi) ?? now;
  slowSinceByMmsi.set(mmsi, slowSince);
  if (now - slowSince < 20 * 60_000) return;
  try {
    await insert("maritime_alerts", [{ mmsi, vessel_name: name ?? null, port, severity: "critical", alert_type: "stopped_vessel", message: "Navire à vitesse très faible (≤ 0,5 kn) depuis au moins 20 minutes ; vérification recommandée.", lat, lon, sog, cog, detected_at: new Date(now).toISOString(), cooldown_key: `${mmsi}:stopped:${Math.floor(now / (30 * 60_000))}` }]);
  } catch (error) { console.error(JSON.stringify({ event: "alert_insert_error", error: String(error) })); }
}

function eventTimestamp(meta, report) {
  const candidate = text(meta.time_utc ?? report.time_utc);
  const parsed = candidate ? Date.parse(candidate) : Number.NaN;
  const now = Date.now();
  if (!Number.isFinite(parsed)) return null;
  if (parsed < Date.parse("2020-01-01T00:00:00Z") || parsed > now + 60 * 60_000) return null;
  return new Date(parsed).toISOString();
}

async function handle(data) {
  if (data.MessageType === "ShipStaticData") {
    const staticData = data.Message?.ShipStaticData ?? {};
    const meta = data.MetaData ?? {};
    const mmsi = String(meta.MMSI ?? staticData.UserID ?? "");
    if (/^\d{9}$/.test(mmsi)) staticByMmsi.set(mmsi, { name: text(staticData.Name ?? meta.ShipName) || undefined, destination: text(staticData.Destination) || undefined });
    return;
  }
  if (data.MessageType !== "PositionReport") return;
  const report = data.Message?.PositionReport ?? {};
  const meta = data.MetaData ?? {};
  const mmsi = String(meta.MMSI ?? report.UserID ?? "");
  const lat = firstCoordinate(report.Latitude, meta.latitude, meta.Latitude);
  const lon = firstCoordinate(report.Longitude, meta.longitude, meta.Longitude);
  if (!/^\d{9}$/.test(mmsi)) return reject("mmsi_invalide");
  if (!validPosition(lat, lon)) return reject("coordonnees_invalides");
  const ts = eventTimestamp(meta, report);
  if (!ts) return reject("timestamp_absent_ou_invalide");
  const sogValue = Number(report.Sog);
  const cogValue = Number(report.Cog);
  const headingValue = Number(report.TrueHeading);
  const navValue = Number(report.NavigationalStatus);
  const sog = Number.isFinite(sogValue) && sogValue >= 0 && sogValue <= 102.3 ? sogValue : null;
  const cog = Number.isFinite(cogValue) && cogValue >= 0 && cogValue <= 360 ? cogValue : null;
  const heading = Number.isFinite(headingValue) && headingValue >= 0 && headingValue <= 360 ? headingValue : null;
  const navStatus = Number.isFinite(navValue) && navValue >= 0 && navValue <= 15 ? navValue : null;
  const receivedAt = new Date().toISOString();
  const name = staticByMmsi.get(mmsi)?.name ?? (text(meta.ShipName) || null);
  const port = inferPort(lat, lon);
  queue.push({ mmsi, ship_name: name, latitude: lat, longitude: lon, sog, cog, heading, nav_status: navStatus, ts, received_at: receivedAt });
  received += 1;
  await evaluateCriticalAlert({ mmsi, name, port, lat, lon, sog, cog });
  if (queue.length >= BATCH_SIZE) await flush();
}

function decode(raw) {
  if (typeof raw === "string") return JSON.parse(raw);
  if (raw instanceof ArrayBuffer) return JSON.parse(new TextDecoder().decode(raw));
  return JSON.parse(String(raw));
}

const socket = new WebSocket(AISSTREAM_URL);
const timer = setInterval(() => void flush(), FLUSH_INTERVAL_MS);
const stop = async (code = 0) => { clearInterval(timer); try { socket.close(); } catch {} await flush(); console.log(JSON.stringify({ event: "worker_stop", received, inserted, rejected, rejectedByReason })); process.exit(code); };
socket.addEventListener("open", () => { socket.send(JSON.stringify({ APIKey: env.aisKey, BoundingBoxes: [BBOX], FilterMessageTypes: ["PositionReport", "ShipStaticData"] })); console.log(JSON.stringify({ event: "ais_connected", bbox: [BBOX], runSeconds: RUN_MS / 1000 })); });
socket.addEventListener("message", (event) => { try { void handle(decode(event.data)); } catch (error) { reject("trame_invalide"); console.error(JSON.stringify({ event: "ais_frame_error", error: String(error) })); } });
socket.addEventListener("error", () => console.error(JSON.stringify({ event: "ais_socket_error" })));
socket.addEventListener("close", (event) => console.log(JSON.stringify({ event: "ais_closed", code: event.code, reason: event.reason || "non fourni" })));
setTimeout(() => void stop(0), RUN_MS);
process.on("SIGINT", () => void stop(0));
process.on("SIGTERM", () => void stop(0));
