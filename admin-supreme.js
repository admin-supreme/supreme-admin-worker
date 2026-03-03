import {  createClient } from "@libsql/client/web";
import { Router } from "itty-router";
const BATCH_SIZE = 1000;
const MAX_FILE_SIZE = 90 * 1024 * 1024; 
const STATE_KEY = "export_state";
const EXPORT_PREFIX = "anime_export/";
const CPU_SAFE_BATCHES = 25; 
const MAX_RUNTIME_MS = 23_000;
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
  router.get("/admin/anime-ids", async () => {
    const db = buildLibsqlClient(env);
    return await getAllAnimeIds(db);
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
    ctx.waitUntil(runFullExport(env));
  }
};
    
async function getAllAnimeIds(db) {
  try {
    const result = await db.execute({
      sql: `SELECT id FROM anime_info`
    });

    if (!result || !Array.isArray(result.rows)) {
      return json(
        { error: "Unexpected database response format" },
        500
      );
    }

    const ids = result.rows.map(row => row.id);

    return json({
      success: true,
      total: ids.length,
      data: ids
    });

  } catch (err) {
  console.error("Failed to fetch all anime IDs:", err);
  return json(
    {
      error: "Failed to fetch all anime IDs",
      details: err instanceof Error ? err.message : "Unknown error"
    },
    500
  );
}
}
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
    await db.execute({sql:"BEGIN"});

    const fields = Object.keys(anime_info);
    const placeholders = fields.map(() => "?").join(", ");

        await db.execute({
      sql: `
        INSERT INTO anime_info (${fields.join(", ")})
        VALUES (${placeholders})
      `,
      args: fields.map(f => anime_info[f])
    });


    for (const ep of streaming_links || []) {
      await db.execute({
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

    await db.execute({sql:"COMMIT"});
ctx.waitUntil(runFullExport(env));
return json({ success: true });

  } catch (err) {
    try { await db.execute({ sql: "ROLLBACK" }); } catch {}
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
anime_info.studios = anime_info.studios || "";
anime_info.producers = anime_info.producers || "";
anime_info.licensors = anime_info.licensors || "";
anime_info.tags = anime_info.tags || "";
anime_info.themes = anime_info.themes || "";
anime_info.demographics = anime_info.demographics || "";
anime_info.title_synonyms = anime_info.title_synonyms || "";
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
  // Basic Validation
  for (const ep of streaming_links) {
    if (!Number.isInteger(ep.episode_number) || ep.episode_number < 1) {
      return json({ error: "Invalid episode number" }, 400);
    }
    if (!ep.stream_url) {
      return json({ error: "Stream URL required" }, 400);
    }
  }

  try {
    await db.execute({sql:"BEGIN"});
    
    /* ================= UPDATE ANIME INFO ================= */

    const fields = Object.keys(anime_info)
      .filter(f => f !== "created_at");
if (fields.length === 0) {
  return json({ error: "No fields to update" }, 400);
}
    const setClause = fields.map(f => `${f} = ?`).join(", ");

    const values = fields.map(f => anime_info[f]);

    await db.execute({
  sql: `
      UPDATE anime_info
      SET ${setClause},
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `,   args: [...values, id]
});

    /* ================= SMART DIFF EPISODES ================= */

    const existingResult = await db.execute({
  sql: `SELECT * FROM streaming_link WHERE anime_id = ?`,
  args: [id]
    });

const existing = existingResult.rows;

    const existingMap = new Map();
    existing.forEach(e => {
      existingMap.set(e.episode_number, e);
    });

    const incomingMap = new Map();
    streaming_links.forEach(e => {
      incomingMap.set(e.episode_number, e);
    });

    // 1️⃣ DELETE removed episodes
    for (const [epNum, row] of existingMap.entries()) {
  if (!incomingMap.has(epNum)) {
    await db.execute({
      sql: `
        DELETE FROM streaming_link
        WHERE anime_id = ? AND episode_number = ?
      `,
      args: [id, epNum]
    });
  }
}
    // 2️⃣ INSERT or UPDATE
    for (const [epNum, ep] of incomingMap.entries()) {
      const existingRow = existingMap.get(epNum);

      if (!existingRow) {
        await db.execute({
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
      } else {
        if (isEpisodeChanged(existingRow, ep)) {
          await db.execute({
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
    }
    await db.execute({sql:"COMMIT"});
ctx.waitUntil(runFullExport(env));
return json({ success: true });

  } catch (err) {
    try { await db.execute({ sql: "ROLLBACK" }); } catch {}
    console.error(err);
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
  const existingLock = await env.EXPORT_STATE.get("lock");
if (existingLock) {
  console.log("⚠️ Export already running");
  return;
}

// write unique lock token
const lockToken = crypto.randomUUID();

await env.EXPORT_STATE.put("lock", lockToken, {
  expirationTtl: 60 * 60,
});

// verify we still own the lock
const verifyLock = await env.EXPORT_STATE.get("lock");
if (verifyLock !== lockToken) {
  console.log("⚠️ Lost lock race");
  return;
}
  try {
    const db = buildDb(env);
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
    while (true) {
  if (Date.now() - startTime > MAX_RUNTIME_MS) {
    break;
  }

  const result = await db.execute({
  sql: `
    SELECT ai.*
    FROM anime_info ai
    WHERE ai.id > ?
    AND EXISTS (
      SELECT 1
      FROM streaming_link sl
      WHERE sl.anime_id = ai.id
    )
    ORDER BY ai.id
    LIMIT ?
  `,
  args: [lastId, BATCH_SIZE],
});

  const rows =
  result &&
  Array.isArray(result.rows)
    ? result.rows
    : [];

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

  break;
}

    const rowString = JSON.stringify(row);
    const rowSize = encoder.encode(rowString).length;

    // 📦 flush if file full
    if (currentSize + rowSize > MAX_FILE_SIZE) {
      await uploadChunk(env, fileIndex, buffer);
      fileIndex += 1;
      buffer = [];
      currentSize = 0;
    }

    buffer.push(row);
    currentSize += rowSize;
    lastId = row.id;
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

// Cleanup old files if finished
const isFinished = reachedEnd;
if (isFinished) {
  await cleanupOldParts(env, fileIndex - 1);
  await env.EXPORT_STATE.delete(STATE_KEY);
  return;
}
  await env.EXPORT_STATE.put(
  STATE_KEY,
  JSON.stringify({ lastId, fileIndex }),
  { expirationTtl: 60 * 60 * 24 * 7 }
);
  } finally {
    await env.EXPORT_STATE.delete("lock");
  }
}

function buildDb(env) {
  return createClient({
    url: env.TURSO_DATABASE_URL.trim(),
    authToken: env.TURSO_AUTH_TOKEN.trim(),
  });
}

async function uploadChunk(env, index, rows) {
  const content = JSON.stringify(rows);
const base64Content = toBase64Utf8(content);

  const path = `${EXPORT_PREFIX}anime_part_${index}.json`;

  // Try to fetch existing file SHA
  const getFile = await fetch(
    `https://api.github.com/repos/${env.GITHUB_REPO}/contents/${path}`,
    {
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json"
      }
    }
  );
  let sha = null;
  if (getFile.ok) {
    const fileData = await getFile.json();
    sha = fileData.sha;
  }

  // Upload or update file
let response;
for (let attempt = 1; attempt <= 3; attempt++) {
  response = await fetch(
    `https://api.github.com/repos/${env.GITHUB_REPO}/contents/${path}`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json"
      },
      body: JSON.stringify({
        message: `Auto export anime_part_${index}`,
        content: base64Content,
        ...(sha && { sha })
      })
    }
  );

  if (response.ok) break;

if (
  response.status === 403 ||
  response.status === 409 ||
  response.status === 429 ||
  response.status >= 500
) {
  await new Promise(r => setTimeout(r, attempt * 5000));
  continue;
}

  if (attempt === 3) {
    const err = await response.text();
    throw new Error(`GitHub upload failed for part ${index}: ${err}`);
  }

  await new Promise(r => setTimeout(r, attempt * 2000));
}
  }
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
    `https://api.github.com/repos/${env.GITHUB_REPO}/contents/anime_export`,
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
    const match = file.name.match(/anime_part_(\d+)\.json/);
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