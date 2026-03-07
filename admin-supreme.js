import {  createClient } from "@libsql/client/web";
import { Router } from "itty-router";
import { ExportLock } from "./ExportLock.js";
const BATCH_SIZE = 300;
const MAX_FILE_SIZE = 650 * 1024;
const STATE_KEY = "export_state";
const EXPORT_PREFIX = "anime/";
const MAX_RUNTIME_MS = 23_000;
function generateToken(){
const chars="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789#%";
let token="";
for(let i=0;i<5;i++){
token+=chars[Math.floor(Math.random()*chars.length)];
}
return token;
}
async function uploadIndexChunk(env, rows) {
  const content = rows.map(r => JSON.stringify(r)).join("\n");
  const base64 = toBase64Utf8(content);

  if (!env.__exportFiles) env.__exportFiles = [];

  const indexId = crypto.randomUUID();

  env.__exportFiles.push({
    path: `index/index_${indexId}.ndjson`,
    content: base64
  });
}

async function commitExport(env) {
  const files = env.__exportFiles || [];
  if (files.length === 0) return;

  const headers = {
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    Accept: "application/vnd.github+json"
  };

  const repo = env.GITHUB_REPO;

  const refRes = await fetch(
    `https://api.github.com/repos/${repo}/git/ref/heads/main`,
    { headers }
  );
  const refData = await refRes.json();
  const latestCommitSha = refData.object.sha;

  const commitRes = await fetch(
    `https://api.github.com/repos/${repo}/git/commits/${latestCommitSha}`,
    { headers }
  );
  const commitData = await commitRes.json();
  const baseTree = commitData.tree.sha;

  const treeItems = files.map(file => ({
  path: file.path,
  mode: "100644",
  type: "blob",
  content: new TextDecoder().decode(
    Uint8Array.from(atob(file.content), c => c.charCodeAt(0))
  )
}));
  const treeRes = await fetch(
    `https://api.github.com/repos/${repo}/git/trees`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        base_tree: baseTree,
        tree: treeItems
      })
    }
  );

  const treeData = await treeRes.json();

  const newCommitRes = await fetch(
    `https://api.github.com/repos/${repo}/git/commits`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        message: "Auto export dataset",
        tree: treeData.sha,
        parents: [latestCommitSha]
      })
    }
  );

  const newCommit = await newCommitRes.json();

  await fetch(
    `https://api.github.com/repos/${repo}/git/refs/heads/main`,
    {
      method: "PATCH",
      headers,
      body: JSON.stringify({ sha: newCommit.sha })
    }
  );

  env.__exportFiles = [];
}
function buildLibsqlClient(env) {
const url = env.TURSO_DATABASE_URL && env.TURSO_DATABASE_URL.trim();
const authToken = env.TURSO_AUTH_TOKEN && env.TURSO_AUTH_TOKEN.trim();
  
  if (!url) {
  throw new Error("TURSO_DATABASE_URL env var is not defined");
}
if (!authToken) {
  throw new Error("TURSO_AUTH_TOKEN env var is not defined");
}

return createClient({ url, authToken });}
function buildRouter(env) {
  const router = Router();
  
  router.options("*", () =>
  new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    }
  })
);
  router.post("/trigger-export", async (req, env, ctx) => {
  const auth = req.headers.get("Authorization");
  if (auth !== `Bearer ${env.EXPORT_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  ctx.waitUntil(runFullExport(env));
  return json({ started: true });
});
router.get("/admin/search", async (request) => {
  const db = buildLibsqlClient(env);
  const url = new URL(request.url);
  const query = url.searchParams.get("q");

  if (!query || query.length < 2) {
    return json({ data: [] });
  }

  const normalized = query.trim();

  const result = await db.execute({
    sql: `
      SELECT id, title, year
      FROM anime_info
     WHERE title LIKE ?
      ORDER BY popularity DESC
      LIMIT 20
    `,
    args: [`%${normalized}%`]
  });

  return json({ data: result.rows });
});
  router.post("/admin/anime", async (request, env, ctx) => {
  const db = buildLibsqlClient(env);
  return await createAnime(request, db, env, ctx);
});

  router.get("/admin/anime/:id", async (request) => {
  const db = buildLibsqlClient(env);
  const id = request.params.id;
  return await getAnime(db, id);
});
router.post("/admin/token/:id", async (request, env) => {

  const id = request.params.id;

  const draft = await env.KV.get(`draft:${id}`);

  if (!draft) {
    return json({ error: "No draft found" }, 404);
  }

  // delete previous token
  const oldToken = await env.KV.get(`anime_token:${id}`);
  if (oldToken) {
    await env.KV.delete(`token:${oldToken}`);
  }
  let token;
  let exists = true;

  while (exists) {
    token = generateToken();
    exists = await env.KV.get(`token:${token}`);
  }
  const parsed = JSON.parse(draft);
parsed.id = id;

await env.KV.put(`token:${token}`, JSON.stringify(parsed));
  await env.KV.put(`anime_token:${id}`, token);

  return json({ token });

});

router.get("/admin/token/:token", async (request, env) => {
  const token = request.params.token;
  const data = await env.KV.get(`token:${token}`);

  if (!data) {
    return json({ error: "Invalid token" }, 404);
  }

  return json(JSON.parse(data));
});
  router.put("/admin/anime/:id", async (request, env, ctx) => {
  const db = buildLibsqlClient(env);
  const id = request.params.id;
  return await updateAnime(request, db, id, env, ctx);
});

router.delete("/admin/anime/:id", async (request) => {
  const db = buildLibsqlClient(env);
  const id = request.params.id;
  return await deleteAnime(db, id);
});
  router.post("/admin/cache/:id", async (request, env) => {
  const id = request.params.id;
  const body = await request.text();
  await env.KV.put(
    `draft:${id}`,
    body,
    { expirationTtl: 86400 }
  );
  return json({ success: true });
});
  router.all("*", () =>
    json({ error: "Not Found" }, 404)
  );
  return router;
}
  export { ExportLock };
export default {
  async fetch(request, env, ctx) {
    if (env.router === undefined) {
      env.router = buildRouter(env);
    }
    return env.router.fetch(request, env, ctx);
  },

  async scheduled(event, env, ctx) {
  }
};
async function getAnime(db, id) {
  const animeResult = await db.execute({
    sql: `SELECT * FROM anime_info WHERE id = ?`,
    args: [id]
  });

  const anime = flattenAnimeFields(animeResult.rows[0]);

  if (!anime) {
    return json({ error: "Anime not found" }, 404);
  }

  // Get episodes
  const episodesResult = await db.execute({
    sql: `
      SELECT episode_number, episode_title, quality, language,
             server_name, stream_url, download_url
      FROM streaming_link
      WHERE anime_id = ?
      ORDER BY episode_number ASC
    `,
    args: [id]
  });

  const episodes = episodesResult.rows;

  return json({
    anime_info: anime,
    streaming_links: episodes
  });
}
function flattenAnimeFields(anime) {
  const copy = { ...anime };
  if (copy.studios) {
    copy.studios = Array.isArray(copy.studios)
      ? copy.studios.map(s => s.name).join(", ")
      : copy.studios;
  }

  if (copy.producers) {
    copy.producers = Array.isArray(copy.producers)
      ? copy.producers.map(p => p.name).join(", ")
      : copy.producers;
  }

  if (copy.licensors) {
    copy.licensors = Array.isArray(copy.licensors)
      ? copy.licensors.map(l => l.name).join(", ")
      : copy.licensors;
  }

  if (copy.tags) {
    copy.tags = Array.isArray(copy.tags) ? copy.tags.join(", ") : copy.tags;
  }

  if (copy.themes) {
    copy.themes = Array.isArray(copy.themes) ? copy.themes.join(", ") : copy.themes;
  }

  if (copy.demographics) {
    copy.demographics = Array.isArray(copy.demographics)
      ? copy.demographics.join(", ")
      : copy.demographics;
  }

  // title_synonyms: simple comma-separated
  if (copy.title_synonyms) {
    copy.title_synonyms = Array.isArray(copy.title_synonyms)
      ? copy.title_synonyms.join(", ")
      : copy.title_synonyms;
  }

  return copy;
}
async function deleteAnime(db, id) {
  const result = await db.execute({
    sql: `DELETE FROM anime_info WHERE id = ?`,
    args: [id]
  });

  if (result.rowsAffected === 0) {
    return json({ error: "Anime not found" }, 404);
  }

  return json({ success: true });
}
async function createAnime(request, db, env, ctx) {
  const body = await request.json();
  const { anime_info, streaming_links } = body;
anime_info.studios = anime_info.studios || "";
anime_info.producers = anime_info.producers || "";
anime_info.licensors = anime_info.licensors || "";
anime_info.tags = anime_info.tags || "";
anime_info.themes = anime_info.themes || "";
anime_info.demographics = anime_info.demographics || "";
anime_info.title_synonyms = anime_info.title_synonyms || "";
  if (!anime_info || !anime_info.id) {
    return json({ error: "Anime ID required" }, 400);
  }
  try {
  const queries = [];
  const fields = Object.keys(anime_info);
  const placeholders = fields.map(() => "?").join(", ");
  queries.push({
    sql: `
      INSERT INTO anime_info (${fields.join(", ")})
      VALUES (${placeholders})
    `,
    args: fields.map(f => anime_info[f])
  });
  for (const ep of streaming_links || []) {
    queries.push({
      sql: `
        INSERT INTO streaming_link
        (anime_id, episode_number, episode_title,
         quality, language, server_name,
         stream_url, download_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `,
      args: [
        anime_info.id,
        ep.episode_number,
        ep.episode_title || "",
        ep.quality || "",
        ep.language || "",
        ep.server_name || "",
        ep.stream_url,
        ep.download_url || ""
      ]
    });
  }

  await db.batch(queries, "write");
  return json({ success: true });
} catch (err) {
  console.error("Creation failed:", err);
  return json({ error: "Creation failed" }, 500);
}
}
async function updateAnime(request, db, id, env, ctx) {
  const body = await request.json();
  const { anime_info, streaming_links } = body;

  if (!anime_info || !streaming_links) {
    return json({ error: "Invalid payload" }, 400);
  }

  anime_info.id = id;

  // Normalize string fields
  anime_info.studios = anime_info.studios || "";
  anime_info.producers = anime_info.producers || "";
  anime_info.licensors = anime_info.licensors || "";
  anime_info.tags = anime_info.tags || "";
  anime_info.themes = anime_info.themes || "";
  anime_info.demographics = anime_info.demographics || "";
  anime_info.title_synonyms = anime_info.title_synonyms || "";
  // Type conversion
  const integerFields = [
    "mal_id", "year", "episodes", "total_seasons",
    "popularity", "rank", "scored_by",
    "members", "favorites"
  ];

  const realFields = ["rating"];

  for (const field of integerFields) {
    if (anime_info[field] !== undefined) {
      anime_info[field] = Number(anime_info[field]) || null;
    }
  }

  for (const field of realFields) {
    if (anime_info[field] !== undefined) {
      anime_info[field] = parseFloat(anime_info[field]) || null;
    }
  }

  // Episode validation
  for (const ep of streaming_links) {
    if (!Number.isInteger(ep.episode_number) || ep.episode_number < 1) {
      return json({ error: "Invalid episode number" }, 400);
    }
    if (!ep.stream_url) {
      return json({ error: "Stream URL required" }, 400);
    }
  }
try {

  const queries = [];

  const fields = Object.keys(anime_info).filter(f => f !== "created_at");

  if (fields.length === 0) {
    throw new Error("No fields to update");
  }

  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => anime_info[f]);

  queries.push({
    sql: `
      UPDATE anime_info
      SET ${setClause},
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `,
    args: [...values, id]
  });

  const existingResult = await db.execute({
    sql: `
      SELECT episode_number,
             episode_title,
             quality,
             language,
             server_name,
             stream_url,
             download_url
      FROM streaming_link
      WHERE anime_id = ?
    `,
    args: [id]
  });

  const existing = existingResult.rows || [];
  const existingMap = new Map();
  for (const e of existing) {
    existingMap.set(e.episode_number, e);
  }

  const incomingMap = new Map();
  for (const e of streaming_links) {
    incomingMap.set(e.episode_number, e);
  }

  for (const [epNum] of existingMap.entries()) {
    if (!incomingMap.has(epNum)) {
      queries.push({
        sql: `
          DELETE FROM streaming_link
          WHERE anime_id = ? AND episode_number = ?
        `,
        args: [id, epNum]
      });
    }
  }

  for (const [epNum, ep] of incomingMap.entries()) {

    const existingRow = existingMap.get(epNum);

    if (!existingRow) {

      queries.push({
        sql: `
          INSERT INTO streaming_link
          (anime_id, episode_number, episode_title,
           quality, language, server_name,
           stream_url, download_url)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `,
        args: [
          id,
          ep.episode_number,
          ep.episode_title || "",
          ep.quality || "",
          ep.language || "",
          ep.server_name || "",
          ep.stream_url,
          ep.download_url || ""
        ]
      });

    } else if (isEpisodeChanged(existingRow, ep)) {

      queries.push({
        sql: `
          UPDATE streaming_link
          SET episode_title = ?,
              quality = ?,
              language = ?,
              server_name = ?,
              stream_url = ?,
              download_url = ?
          WHERE anime_id = ?
          AND episode_number = ?
        `,
        args: [
          ep.episode_title || "",
          ep.quality || "",
          ep.language || "",
          ep.server_name || "",
          ep.stream_url,
          ep.download_url || "",
          id,
          epNum
        ]
      });

    }
  }

  await db.batch(queries, "write");

  return json({ success: true });

} catch (err) {

  console.error("Update failed:", err);

  return json({ error: "Update failed" }, 500);

}
}
function isEpisodeChanged(oldRow, newRow) {
  return (
    oldRow.episode_title !== (newRow.episode_title || "") ||
    oldRow.quality !== (newRow.quality || "") ||
    oldRow.language !== (newRow.language || "") ||
    oldRow.server_name !== (newRow.server_name || "") ||
    oldRow.stream_url !== newRow.stream_url ||
    oldRow.download_url !== (newRow.download_url || "")
  );
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    }
  });
}
async function runFullExport(env) {
  const id = env.EXPORT_LOCK.idFromName("global-lock");
const stub = env.EXPORT_LOCK.get(id);

const lock = await stub.fetch("https://lock/lock");

if (!lock || lock.status !== 200) {
  console.log("Export already running or lock failed");
  return;
}

  try {
    const db = buildLibsqlClient(env);
const startTime = Date.now();

  const encoder = new TextEncoder();
  let rawState = await env.EXPORT_STATE.get(STATE_KEY);
let state;

try {
  state = rawState ? JSON.parse(rawState) : null;
} catch {
  state = null;
}

if (!state || typeof state !== "object") {
  state = { lastId: "", fileIndex: 1 };
}

let { lastId, fileIndex } = state;

let buffer = [];
let currentSize = 0;
let reachedEnd = false;
let runtimeLimitHit = false;
let searchIndexBuffer = [];
let searchIndexSize = 0;
    while (true) {
  if (Date.now() - startTime > MAX_RUNTIME_MS) {
    break;
  }

const result = await db.execute({
  sql: `
    SELECT DISTINCT ai.*
FROM anime_info ai
JOIN streaming_link sl
ON sl.anime_id = ai.id
WHERE ai.id > ?
ORDER BY ai.id
LIMIT ?
  `,
  args: [lastId, BATCH_SIZE],
});

  const rows = result?.rows ?? [];

  if (!rows.length) {
    reachedEnd = true;
    break;
  }

  for (const row of rows) {
    // ⏱️ check time inside row loop too
    if (Date.now() - startTime > MAX_RUNTIME_MS) {

  console.log("⏱️ Runtime limit approaching — flushing buffer");

  if (buffer.length > 0) {
    await uploadChunk(env, fileIndex, buffer);
    fileIndex += 1;
    buffer = [];
    currentSize = 0;
  }

  runtimeLimitHit = true;
  break;
}

    const rowString = JSON.stringify(row);
const estimatedSize = encoder.encode(rowString + ",").length;
const rowSize = estimatedSize;
    
    if (currentSize + rowSize + 2 > MAX_FILE_SIZE) {
  await uploadChunk(env, fileIndex, buffer);
  fileIndex += 1;

  buffer = [];
  currentSize = 0;
}

    buffer.push(row);
currentSize += rowSize;
lastId = row.id;
    
const indexRow = {
  id: row.id,
  title: row.title,
  year: row.year,
  type: row.type,
  part: fileIndex
};

searchIndexBuffer.push(indexRow);
searchIndexSize++;

if (searchIndexSize >= 2000) {
  await uploadIndexChunk(env, searchIndexBuffer);
  searchIndexBuffer = [];
  searchIndexSize = 0;
}
  }
      if (runtimeLimitHit) {
  break;
}
  if (rows.length < BATCH_SIZE) {
    reachedEnd = true;
    break;
  }
}
if (buffer.length > 0) {
  await uploadChunk(env, fileIndex, buffer);
  fileIndex += 1;
  buffer = [];
  currentSize = 0;
}
const isFinished = reachedEnd;

if (isFinished) {

  if (searchIndexBuffer.length > 0) {
    await uploadIndexChunk(env, searchIndexBuffer);
  }
  await commitExport(env);
  await cleanupOldParts(env, fileIndex - 1);
  await env.EXPORT_STATE.delete(STATE_KEY);
  await env.EXPORT_STATE.delete("search_index_state");
  return;
}
  await env.EXPORT_STATE.put(
  STATE_KEY,
  JSON.stringify({ lastId, fileIndex }),
  { expirationTtl: 60 * 60 * 24 * 7 }
);
    
  } finally {
    await stub.fetch("https://lock/unlock");
  }
}

async function uploadChunk(env, index, rows) {
  const content = JSON.stringify(rows);
  const base64Content = toBase64Utf8(content);

  const path = `${EXPORT_PREFIX}part_${index}.json`;

  if (!env.__exportFiles) {
    env.__exportFiles = [];
  }

  env.__exportFiles.push({
    path,
    content: base64Content
  });
}
function toBase64Utf8(str) {
  const bytes = new TextEncoder().encode(str);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
  async function cleanupOldParts(env, latestIndex) {
  const list = await fetch(
    `https://api.github.com/repos/${env.GITHUB_REPO}/contents/anime`,
    {
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json"
      }
    }
  );

  if (!list.ok) return;

  const files = await list.json();

  for (const file of files) {
    const match = file.name.match(/part_(\d+)\.json/);
    if (!match) continue;

    const index = parseInt(match[1]);
    if (index > latestIndex) {
      await fetch(file.url, {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${env.GITHUB_TOKEN}`,
          Accept: "application/vnd.github+json"
        },
        body: JSON.stringify({
          message: `Cleanup old ${file.name}`,
          sha: file.sha
        })
      });
    }
  }
}
async function uploadIndex(env, indexData) {

  const repo = env.GITHUB_REPO;

  const headers = {
    Authorization: `Bearer ${env.GITHUB_TOKEN}`,
    "Content-Type": "application/json"
  };

  // Convert to compact NDJSON
  const ndjson = indexData
    .map(row => JSON.stringify({
      i: row.id,        
      t: row.title,       
      y: row.year,
      f: row.part    
    }))
    .join("\n");

  const contentBase64 = toBase64Utf8(ndjson);

  const res = await fetch(
    `https://api.github.com/repos/${repo}/contents/index.json`,
    {
      method: "PUT",
      headers,
      body: JSON.stringify({
        message: "Update anime search index",
        content: contentBase64
      })
    }
  );

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Index upload failed: ${text}`);
  }

  console.log("✅ Search index uploaded");
}
