import {  createClient } from "@libsql/client/web";
import { Router } from "itty-router";
function buildLibsqlClient(env) {
const url = env.TURSO_DATABASE_URL && env.TURSO_DATABASE_URL.trim();
const authToken = env.TURSO_AUTH_TOKEN && env.TURSO_AUTH_TOKEN.trim(); // add this line
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

  router.post("/admin/anime", async (request) => {
    const db = buildLibsqlClient(env);
    return await createAnime(request, db);
  });

  router.get("/admin/anime/:id", async (request) => {
  const db = buildLibsqlClient(env);
  const id = request.params.id;
  return await getAnime(db, id);
});

  router.put("/admin/anime/:id", async (request) => {
  const db = buildLibsqlClient(env);
  const id = request.params.id;
  return await updateAnime(request, db, id);
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
export default { async fetch(request, env) {
    if (env.router === undefined) {
      env.router = buildRouter(env);
    }
    return env.router.fetch(request);
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
async function createAnime(request, db) {
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
    return json({ success: true });

  } catch (err) {
    try { await db.execute({ sql: "ROLLBACK" }); } catch {}
    return json({ error: "Creation failed" }, 500);
  }
}
async function updateAnime(request, db, id) {
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
