import { createClient } from "@libsql/client/web";
export default {
  async fetch(request, env, ctx) {
    return new Response("Worker is running (cron only).", {
      status: 200,
    });
  },

  async scheduled(event, env, ctx) {
    const db = createClient({
      url: env.TURSO_DATABASE_URL,
      authToken: env.TURSO_AUTH_TOKEN,
    });

    await syncJikan(env, db);
    await refreshMissingImages(env, db);
  }
};

async function syncJikan(env, db) {

  // 1️⃣ Get current page from KV
  let page = parseInt(await env.STATE.get("jikan_page") || "1");
let offset = parseInt(await env.STATE.get("jikan_offset") || "0");

const BATCH_SIZE = offset === 0 ? 13 : 12; // safe for CF limits

  console.log("Fetching Jikan page:", page);

  const result = await fetchJikan(page);
  const mediaList = result.data;
  const hasNext = result.pagination?.has_next_page;

  if (!mediaList || mediaList.length === 0) {
    // Reset to page 1 if something strange happens
    await env.STATE.put("jikan_page", "1");
    return;
  }

  const batch = mediaList.slice(offset, offset + BATCH_SIZE);

for (const media of batch) {
    try {
      const transformed = transform(media);

      const tmdbPoster = await fetchHighResPoster(
        env,
        transformed.title,
        transformed.year
      );

      if (tmdbPoster) {
        transformed.image_url = tmdbPoster;
      } else {
        transformed.image_url =
          media.images?.jpg?.large_image_url || null;
      }

      await upsertAnime(db, transformed);

    } catch (err) {
      console.error("UPSERT FAILED:", err);
    }
  }

  // 2️⃣ Decide next page
  const newOffset = offset + BATCH_SIZE;

if (newOffset >= mediaList.length) {
  // Finished this page → move to next page
  const nextPage = hasNext ? page + 1 : 1;

  await env.STATE.put("jikan_page", String(nextPage));
  await env.STATE.put("jikan_offset", "0");
} else {
  // Still items remaining on same page
  await env.STATE.put("jikan_offset", String(newOffset));
}
}
async function refreshMissingImages(env, db) {
  const result = await db.execute({
  sql: `
    SELECT id, title, year, image_url
    FROM anime_info
    WHERE image_url IS NULL
       OR image_url NOT LIKE '%image.tmdb.org%'
    LIMIT 50
  `
});

const results = result.rows;

  for (const anime of results) {
    try {

      const tmdbPoster = await fetchHighResPoster(
        env,
        anime.title,
        anime.year
      );

      if (!tmdbPoster) continue;

      await db.execute({
  sql: `
    UPDATE anime_info
    SET image_url = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `,
  args: [tmdbPoster, anime.id]
});

    } catch (err) {
      console.log("REFRESH FAILED:", anime.title);
    }
  }
}
async function fetchJikan(page) {
  try {
    const res = await fetch(
      `https://api.jikan.moe/v4/top/anime?page=${page}`
    );

    if (!res.ok) return { data: [], pagination: null };

    return await res.json();

  } catch (err) {
    return { data: [], pagination: null };
  }
}
async function fetchHighResPoster(env, title, year) {

  const query = encodeURIComponent(
  year ? `${title} ${year}` : title
);

  // Search TV first
  const tvUrl = `https://api.themoviedb.org/3/search/tv?api_key=${env.TMDB_API_KEY}&query=${query}`;

  const tvRes = await fetch(tvUrl);
  if (tvRes.ok) {
    const tvData = await tvRes.json();

    const animeTv = tvData.results?.find(r =>
  r.poster_path &&
  r.genre_ids?.includes(16) &&
  (!year || r.first_air_date?.startsWith(String(year)))
);

    if (animeTv) {
      return `https://image.tmdb.org/t/p/original${animeTv.poster_path}`;
    }
  }

  // Search Movie fallback
  const movieUrl = `https://api.themoviedb.org/3/search/movie?api_key=${env.TMDB_API_KEY}&query=${query}`;

  const movieRes = await fetch(movieUrl);
  if (!movieRes.ok) return null;

  const movieData = await movieRes.json();

const animeMovie = movieData.results?.find(r =>
  r.poster_path &&
  r.genre_ids?.includes(16) &&
  (!year || r.release_date?.startsWith(String(year)))
);

  if (!animeMovie) return null;

  return `https://image.tmdb.org/t/p/original${animeMovie.poster_path}`;
}

function transform(media) {

  const title = media.title_english || media.title;
  const slug = generateSlug(title) + "-" + media.mal_id;

  return {
    // PRIMARY
    id: slug,
    mal_id: media.mal_id,

    // ===== EXISTING ANILIST COLUMN MAPPINGS =====
    title: title,
    year: media.year || null,
    type: media.type || null,
    overview: media.synopsis || null,
    studio: media.studios?.[0]?.name || null,

    episodes: media.episodes || 0,
    duration: media.duration || null,

    audio: "SUB",
    dubbed_languages: null,

    rating: media.score || null,
    popularity: media.popularity || null,

    top_genre_rank: media.rank
      ? `Top #${media.rank}`
      : null,

    airing_status: mapJikanStatus(media.status),

    airing_date: media.aired?.from
      ? media.aired.from.split("T")[0]
      : null,

    tags: JSON.stringify(
      (media.genres || []).map(g => g.name)
    ),

    total_seasons: 1,

    // ===== ADDITIONAL JIKAN FIELDS =====
    title_japanese: media.title_japanese,
    title_synonyms: JSON.stringify(media.title_synonyms || []),

    source: media.source,
    age_rating: media.rating,

    scored_by: media.scored_by,
    rank: media.rank,
    members: media.members,
    favorites: media.favorites,

    season: media.season,
    airing: media.airing ? 1 : 0,

    ended_date: media.aired?.to
      ? media.aired.to.split("T")[0]
      : null,

    studios: JSON.stringify(media.studios || []),
    producers: JSON.stringify(media.producers || []),
    licensors: JSON.stringify(media.licensors || []),

    themes: JSON.stringify(media.themes || []),
    demographics: JSON.stringify(media.demographics || []),

    trailer: media.trailer?.youtube_id || null,
    image_url: null
  };
}

function mapJikanStatus(status) {
  if (status === "Currently Airing") return "AIRING";
  if (status === "Finished Airing") return "COMPLETED";
  if (status === "Not yet aired") return "UPCOMING";
  return null;
}

function generateSlug(title) {
  return title
    .toLowerCase()
    .replace(/[^\w\s]/g, "")
    .replace(/\s+/g, "-");
}
async function upsertAnime(db, anime) {

  await db.execute({
  sql: `
    INSERT INTO anime_info (
      id,
      type,
      title,
      title_japanese,
      title_synonyms,
      mal_id,
      year,
      season,
      studio,
      studios,
      audio,
      dubbed_languages,
      duration,
      episodes,
      tags,
      age_rating,
      total_seasons,
      airing_date,
      ended_date,
      airing_status,
      image_url,
      overview,
      producers,
      licensors,
      themes,
      demographics,
      trailer,
      source,
      popularity,
      rating,
      rank,
      top_genre_rank,
      scored_by,
      members,
      favorites
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)

    ON CONFLICT(mal_id) DO UPDATE SET
      id = excluded.id,
      type = excluded.type,
      title = excluded.title,
      title_japanese = excluded.title_japanese,
      title_synonyms = excluded.title_synonyms,
      year = excluded.year,
      season = excluded.season,
      studio = excluded.studio,
      studios = excluded.studios,
      duration = excluded.duration,
      episodes = excluded.episodes,
      tags = excluded.tags,
      age_rating = excluded.age_rating,
      airing_date = excluded.airing_date,
      ended_date = excluded.ended_date,
      airing_status = excluded.airing_status,
      image_url = CASE WHEN anime_info.image_url LIKE '%image.tmdb.org%' THEN anime_info.image_url ELSE excluded.image_url END,
      overview = excluded.overview,
      producers = excluded.producers,
      licensors = excluded.licensors,
      themes = excluded.themes,
      demographics = excluded.demographics,
      trailer = excluded.trailer,
      source = excluded.source,
      popularity = excluded.popularity,
      rating = excluded.rating,
      rank = excluded.rank,
      top_genre_rank = excluded.top_genre_rank,
      scored_by = excluded.scored_by,
      members = excluded.members,
      favorites = excluded.favorites,
      updated_at = CURRENT_TIMESTAMP
  `,  args: [
    anime.id,
    anime.type,
    anime.title,
    anime.title_japanese,
    anime.title_synonyms,
    anime.mal_id,
    anime.year,
    anime.season,
    anime.studio,
    anime.studios,
    anime.audio,
    anime.dubbed_languages,
    anime.duration,
    anime.episodes,
    anime.tags,
    anime.age_rating,
    anime.total_seasons,
    anime.airing_date,
    anime.ended_date,
    anime.airing_status,
    anime.image_url,
    anime.overview,
    anime.producers,
    anime.licensors,
    anime.themes,
    anime.demographics,
    anime.trailer,
    anime.source,
    anime.popularity,
    anime.rating,
    anime.rank,
    anime.top_genre_rank,
    anime.scored_by,
    anime.members,
    anime.favorites
  ] 
  });

}
