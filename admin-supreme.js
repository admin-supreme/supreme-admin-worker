import {  createClient } from "@libsql/client/web";
import { Router } from "itty-router";

const PART_STATE = "part_state";
const CRON_LIMIT = 5000;
const EXPORT_POINTER = "export_pointer";
const MAX_FILE_SIZE = 650 * 1024;
function generateToken(){
const chars="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789#%";
let token="";
for(let i=0;i<5;i++){
token+=chars[Math.floor(Math.random()*chars.length)];
}
return token;
}

async function commitExport(env) {
  await new Promise(r => setTimeout(r, 100));
  const files = env.__exportFiles || [];
  if (files.length === 0) return;

  const headers = {
  Authorization: `Bearer ${env.GITHUB_TOKEN}`,
  Accept: "application/vnd.github+json",
  "Content-Type": "application/json",
  "User-Agent": "Supreme-Admin-Worker"
};

  const repo = env.GITHUB_REPO;

  const refRes = await fetch(
  `https://api.github.com/repos/${repo}/git/ref/heads/main`,
  { headers }
);

if (!refRes.ok) {
  const text = await refRes.text();
  throw new Error("GitHub ref fetch failed: " + text);
}
const refData = await refRes.json();
  const latestCommitSha = refData.object.sha;

  const commitRes = await fetch(
  `https://api.github.com/repos/${repo}/git/commits/${latestCommitSha}`,
  { headers }
);

if (!commitRes.ok) {
  const text = await commitRes.text();
  throw new Error("GitHub commit fetch failed: " + text);
}
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

  if (!treeRes.ok) {
  const text = await treeRes.text();
  throw new Error("GitHub tree creation failed: " + text);
}
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

  if (!newCommitRes.ok) {
  const text = await newCommitRes.text();
  throw new Error("GitHub commit creation failed: " + text);
}
const newCommit = await newCommitRes.json();
const refUpdate = await fetch(
  `https://api.github.com/repos/${repo}/git/refs/heads/main`,
  {
    method: "PATCH",
    headers,
    body: JSON.stringify({ sha: newCommit.sha })
  }
);

if (!refUpdate.ok) {
  const text = await refUpdate.text();
  throw new Error("GitHub ref update failed: " + text);
}

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
function normalizeList(value) {
  try {
    if (!value) return JSON.stringify([]);

    let items;

    if (Array.isArray(value)) {
      items = value;
    } 
    else if (typeof value === "string") {
      const trimmed = value.trim();

      // If already JSON array
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        const parsed = JSON.parse(trimmed);
        items = Array.isArray(parsed) ? parsed : [parsed];
      } else {
        items = trimmed.split(",");
      }
    } 
    else {
      items = [value];
    }

    const cleaned = items
      .map(v => {
        if (typeof v === "object" && v !== null && "name" in v) {
          return String(v.name).trim();
        }
        return String(v).trim();
      })
      .filter(Boolean);

    return JSON.stringify([...new Set(cleaned)]);
  } catch {
    return JSON.stringify([]);
  }
}
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
  ctx.waitUntil(runCronExport(env));
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

router.delete("/admin/anime/:id", async (request, env, ctx) => {
  const db = buildLibsqlClient(env);
  const id = request.params.id;
  return await deleteAnime(db, id, env, ctx);
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
export default {
  async fetch(request, env, ctx) {
    if (env.router === undefined) {
      env.router = buildRouter(env);
    }
    return env.router.fetch(request, env, ctx);
  },
async scheduled(event, env, ctx) {
  console.log("Cron triggered:", event.cron);

  ctx.waitUntil(runCronExport(env));
}
};
async function updateSingleAnime(env, anime) {
const headers = {
  Authorization: `Bearer ${env.GITHUB_TOKEN}`,
  Accept: "application/vnd.github+json",
  "User-Agent": "Supreme-Admin-Worker"
};
const repo = env.GITHUB_REPO;
const indexRes = await fetch(
 `https://api.github.com/repos/${repo}/contents/index/index.ndjson`,
 { headers }
);
let index = [];
let indexSha = null;
if (indexRes.ok) {
 const data = await indexRes.json();
 indexSha = data.sha;
 const text = atob(data.content);
 index = text.trim().split("\n").map(l => JSON.parse(l));
}

let existing = index.find(r => r.i == anime.id);
  if (existing) {
  existing.t = anime.title;
  existing.j = anime.title_japanese || "";
  existing.y = anime.year;
}
let part;
if (existing) {
 part = existing.f;
 const partRes = await fetch(
  `https://api.github.com/repos/${repo}/contents/anime/part_${part}.json`,
  { headers }
 );

 const file = await partRes.json();
 const rows = JSON.parse(atob(file.content));

 const idx = rows.findIndex(r => r.id == anime.id);

 if (idx !== -1) rows[idx] = anime;

 const content = toBase64Utf8(JSON.stringify(rows));

 env.__exportFiles = [{
  path: `anime/part_${part}.json`,
  content
 }];
}
else {

let part = 1;
let rows = [];
let foundPart = false;
const parts = [...new Set(index.map(r => r.f))].sort((a,b)=>a-b);
for (const p of parts) {
 const partRes = await fetch(
  `https://api.github.com/repos/${repo}/contents/anime/part_${p}.json`,
  { headers }
 );
 if (!partRes.ok) continue;
 const file = await partRes.json();
 const content = atob(file.content);
 const currentSize = new TextEncoder().encode(content).length;
 const newAnimeSize = new TextEncoder().encode(JSON.stringify(anime)).length;
 if (currentSize + newAnimeSize < MAX_FILE_SIZE) {
   rows = JSON.parse(content);
   rows.push(anime);
   part = p;
   foundPart = true;
   env.__exportFiles = [{
     path: `anime/part_${part}.json`,
     content: toBase64Utf8(JSON.stringify(rows))
   }];
   break;
 }
}
if (!foundPart) {
 part = parts.length ? Math.max(...parts) + 1 : 1;
 rows = [anime];
 env.__exportFiles = [{
  path: `anime/part_${part}.json`,
  content: toBase64Utf8(JSON.stringify(rows))
 }];
}
index.push({
  i: anime.id,
  t: anime.title,
  j: anime.title_japanese || "",
  y: anime.year,
  f: part
});
}
const ndjson = index.map(r => JSON.stringify(r)).join("\n");
env.__exportFiles.push({
 path: `index/index.ndjson`,
 content: toBase64Utf8(ndjson)
});
await commitExport(env);
}
async function removeAnimeFromExport(env, id) {
const headers = {
 Authorization: `Bearer ${env.GITHUB_TOKEN}`,
 Accept: "application/vnd.github+json",
 "User-Agent": "Supreme-Admin-Worker"
};
const repo = env.GITHUB_REPO;
const res = await fetch(
 `https://api.github.com/repos/${repo}/contents/index/index.ndjson`,
 { headers }
);
if (!res.ok) return;
const data = await res.json();
const indexSha = data.sha;
let index = atob(data.content)
 .trim()
 .split("\n")
 .map(l => JSON.parse(l));
const existing = index.find(r => r.i == id);
if (!existing) return;
const part = existing.f;
const partRes = await fetch(
 `https://api.github.com/repos/${repo}/contents/anime/part_${part}.json`,
 { headers }
);
if (!partRes.ok) return;
const partFile = await partRes.json();
const rows = JSON.parse(atob(partFile.content));
const filtered = rows.filter(r => r.id != id);
env.__exportFiles = [];
env.__exportFiles.push({
 path: `anime/part_${part}.json`,
 content: toBase64Utf8(JSON.stringify(filtered))
});
index = index.filter(r => r.i != id);

const ndjson = index.map(r => JSON.stringify(r)).join("\n");

env.__exportFiles.push({
 path: `index/index.ndjson`,
 content: toBase64Utf8(ndjson)
});
await commitExport(env);
}
async function getAnime(db, id) {
  const animeResult = await db.execute({
    sql: `SELECT * FROM anime_info WHERE id = ?`,
    args: [id]
  });

  const row = animeResult.rows[0];

if (!row) {
  return json({ error: "Anime not found" }, 404);
}

const anime = flattenAnimeFields(row);

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
function flattenAnimeFields(anime, displayLimit = 5) {
  const copy = { ...anime };
  const listFields = ["studios", "producers", "licensors", "tags", "themes", "demographics", "title_synonyms"];
  for (const field of listFields) {
    if (!copy[field]) {
      copy[field] = "";
      continue;
    }
    let arr;
    try {
      if (typeof copy[field] === "string") {
        arr = JSON.parse(copy[field]);
      } else {
        arr = copy[field];
      }
      if (!Array.isArray(arr)) arr = [arr];
    } catch {
      arr = [copy[field]];
    }
    const names = arr
      .map(v => {
        if (v && typeof v === "object" && "name" in v) return String(v.name).trim();
        return String(v).trim();
      })
      .filter(Boolean);

    const uniqueNames = [...new Set(names)];

    // Optional display limit
    if (displayLimit && uniqueNames.length > displayLimit) {
      copy[field] = uniqueNames.slice(0, displayLimit).join(", ") + ", …";
    } else {
      copy[field] = uniqueNames.join(", ");
    }
  }

  return copy;
}
async function deleteAnime(db, id, env, ctx) {
  const result = await db.execute({
    sql: `DELETE FROM anime_info WHERE id = ?`,
    args: [id]
  });

  if (result.rowsAffected === 0) {
    return json({ error: "Anime not found" }, 404);
  }

  ctx.waitUntil(removeAnimeFromExport(env, id));
return json({ success: true, export: "scheduled" });
}
async function createAnime(request, db, env, ctx) {
  const body = await request.json();
  const { anime_info, streaming_links } = body;
anime_info.studios = normalizeList(anime_info.studios);
anime_info.producers = normalizeList(anime_info.producers);
anime_info.licensors = normalizeList(anime_info.licensors);
anime_info.tags = normalizeList(anime_info.tags);
anime_info.themes = normalizeList(anime_info.themes);
anime_info.demographics = normalizeList(anime_info.demographics);
anime_info.title_synonyms = normalizeList(anime_info.title_synonyms);
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
        normalizeList(ep.quality),
        normalizeList(ep.language),
        ep.server_name || "",
        ep.stream_url,
        ep.download_url || ""
      ]
    });
  }

await db.batch(queries, "write");
const fresh = await db.execute({
  sql: `SELECT * FROM anime_info WHERE id = ?`,
  args: [anime_info.id]
});

ctx.waitUntil(updateSingleAnime(env, fresh.rows[0]));
return json({ success: true, export: "scheduled" });
} catch (err) {
  console.error("Creation failed:", err);
  return json({ error: "Creation failed" }, 500);
}
}
async function updateAnime(request, db, id, env, ctx) {
  const body = await request.json();
  const { anime_info, streaming_links } = body;

  if (!anime_info || !Array.isArray(streaming_links)) {
  return json({ error: "Invalid payload" }, 400);
}
if (streaming_links.length === 0) {
  ctx.waitUntil(removeAnimeFromExport(env, id));
  return json({ success: true, removed_from_export: true });
}
  anime_info.id = id;
  
  anime_info.studios = normalizeList(anime_info.studios);
anime_info.producers = normalizeList(anime_info.producers);
anime_info.licensors = normalizeList(anime_info.licensors);
anime_info.tags = normalizeList(anime_info.tags);
anime_info.themes = normalizeList(anime_info.themes);
anime_info.demographics = normalizeList(anime_info.demographics);
anime_info.title_synonyms = normalizeList(anime_info.title_synonyms);
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
          normalizeList(ep.quality),
          normalizeList(ep.language),
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
         normalizeList(ep.quality), 
          normalizeList(ep.language),
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
const fresh = await db.execute({
  sql: `SELECT * FROM anime_info WHERE id = ?`,
  args: [id]
});

ctx.waitUntil(updateSingleAnime(env, fresh.rows[0]));
return json({ success: true, export: "scheduled" });

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
async function runCronExport(env) {
const db = buildLibsqlClient(env);
const headers = {
 Authorization: `Bearer ${env.GITHUB_TOKEN}`,
 Accept: "application/vnd.github+json",
 "User-Agent": "Supreme-Admin-Worker"
};
const repo = env.GITHUB_REPO;
const result = await db.execute({
 sql: `
 SELECT ai.*
 FROM anime_info ai
 WHERE EXISTS (
   SELECT 1 FROM streaming_link sl
   WHERE sl.anime_id = ai.id
   LIMIT 1
 )
 ORDER BY ai.updated_at DESC
 LIMIT 2000
`
});
const animes = result.rows;
if (!animes.length) return;
const indexRes = await fetch(
 `https://api.github.com/repos/${repo}/contents/index/index.ndjson`,
 { headers }
);
if (!indexRes.ok) return;
const indexFile = await indexRes.json();
const index = atob(indexFile.content)
 .trim()
 .split("\n")
 .map(l => JSON.parse(l));
const idToPart = new Map();
for (const row of index) {
 idToPart.set(row.i, row.f);
}
const partUpdates = new Map();
for (const anime of animes) {
 const part = idToPart.get(anime.id);
 if (!part) continue;
 if (!partUpdates.has(part)) {
   partUpdates.set(part, []);
 }
 partUpdates.get(part).push(anime);
}
env.__exportFiles = [];
for (const [part, animeList] of partUpdates.entries()) {
 const partRes = await fetch(
  `https://api.github.com/repos/${repo}/contents/anime/part_${part}.json`,
  { headers }
 );
 if (!partRes.ok) continue;
 const file = await partRes.json();
 const rows = JSON.parse(atob(file.content));
 const map = new Map();
 for (const a of animeList) {
   map.set(a.id, a);
 }
 for (let i=0;i<rows.length;i++) {
   const updated = map.get(rows[i].id);
   if (updated) {
     rows[i] = updated;
   }
 }
 env.__exportFiles.push({
  path: `anime/part_${part}.json`,
  content: toBase64Utf8(JSON.stringify(rows))
 });
}
await commitExport(env);
}
async function uploadChunk(env, index, rows) {
  const content = JSON.stringify(rows);
  const base64 = toBase64Utf8(content);
  if (!env.__exportFiles) env.__exportFiles = [];
  env.__exportFiles.push({
    path: `anime/part_${index}.json`,
    content: base64
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
  Accept: "application/vnd.github+json",
  "User-Agent": "Supreme-Admin-Worker" 
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
      await fetch(
  `https://api.github.com/repos/${env.GITHUB_REPO}/contents/anime/${file.name}`,
  {
    method: "DELETE",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "Supreme-Admin-Worker"
    },
    body: JSON.stringify({
      message: `Cleanup old ${file.name}`,
      sha: file.sha
    })
  }
);
    }
  }
}
async function uploadIndex(env, rows) {
const content = rows.map(r => JSON.stringify({
  i: r.id,
  t: r.title,
  j: r.title_japanese || "",
  y: r.year,
  f: r.part
})).join("\n");
  if (!env.__exportFiles) env.__exportFiles = [];
  env.__exportFiles.push({
    path: `index/index.ndjson`,
    content: toBase64Utf8(content)
  });
}