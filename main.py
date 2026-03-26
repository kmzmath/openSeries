import os
from contextlib import asynccontextmanager
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

# ─── Config ─────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("SUPABASE_DB_HOST", "localhost"),
    "port": int(os.getenv("SUPABASE_DB_PORT", 5432)),
    "dbname": os.getenv("SUPABASE_DB_NAME", "postgres"),
    "user": os.getenv("SUPABASE_DB_USER", "postgres"),
    "password": os.getenv("SUPABASE_DB_PASSWORD", ""),
    "sslmode": os.getenv("SUPABASE_DB_SSLMODE", "require"),
}


ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "change-me")

db_pool: pool.ThreadedConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = pool.ThreadedConnectionPool(2, 20, **DB_CONFIG)
    yield
    if db_pool:
        db_pool.closeall()


app = FastAPI(
    title="Open Series - Wild Rift",
    version="2.0.0",
    lifespan=lifespan,
)

# ─── CORS ───────────────────────────────────────────────────

origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Helpers ────────────────────────────────────────────────

def get_conn():
    return db_pool.getconn()


def put_conn(conn):
    db_pool.putconn(conn)


def query(sql: str, params: tuple = ()) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        put_conn(conn)


def query_one(sql: str, params: tuple = ()) -> dict | None:
    rows = query(sql, params)
    return rows[0] if rows else None


def fmt_duration(seconds: int | None) -> str:
    """Formata segundos como MM:SS para exibição."""
    if seconds is None:
        return "00:00"
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


def fmt_datetime_br(value) -> str | None:
    """Formata datetime como DD/MM HH:MM no fuso de São Paulo."""
    if value is None:
        return None
    dt = value
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
    else:
        dt = dt.astimezone(ZoneInfo("America/Sao_Paulo"))
    return dt.strftime("%d/%m %H:%M")


def verify_admin(x_api_key: str = Header(None)):
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(403, "API key inválida")


def build_group_agenda(
    teams: list[dict],
    bracket_group: str,
    starts_at=None,
) -> dict:
    def agenda_sort_key(team: dict):
        seed = team.get("seed")
        tie_breaker = team.get("tie_breaker")
        return (
            seed is None,
            seed if seed is not None else 0,
            tie_breaker is None,
            tie_breaker if tie_breaker is not None else 0,
            team["team_name"].lower(),
        )

    ordered_teams = sorted(teams, key=agenda_sort_key)

    if len(ordered_teams) != 4:
        raise HTTPException(
            400,
            f"O grupo {bracket_group} precisa ter exatamente 4 times para gerar a agenda.",
        )

    group_start_at = starts_at
    group_start_label = fmt_datetime_br(group_start_at)
    group_name = f"Grupo {bracket_group}"

    t1, t2, t3, t4 = ordered_teams
    pairings = [
        (t1, t4),
        (t2, t3),
        (t1, t3),
        (t2, t4),
        (t1, t2),
        (t3, t4),
        (t4, t1),
        (t3, t2),
        (t3, t1),
        (t4, t2),
        (t2, t1),
        (t4, t3),
    ]

    matches = []
    for idx, (home, away) in enumerate(pairings, start=1):
        matches.append({
            "order": idx,
            "home_team_id": home["team_id"],
            "home_team": home["team_name"],
            "home_team_tag": home.get("team_tag"),
            "home_seed": home["seed"],
            "away_team_id": away["team_id"],
            "away_team": away["team_name"],
            "away_team_tag": away.get("team_tag"),
            "away_seed": away["seed"],
            "label": f'{home["team_name"]} x {away["team_name"]}',
        })

    return {
        "bracket_group": bracket_group,
        "group_name": group_name,
        "group_start_at": group_start_at.isoformat() if group_start_at else None,
        "group_start_label": group_start_label,
        "teams": ordered_teams,
        "matches": matches,
    }



def group_series_by_best_of(series_list: list[dict]) -> dict:
    grouped = {"MD1": [], "MD3": [], "MD5": []}
    for item in series_list:
        key = f"MD{item['best_of']}"
        grouped.setdefault(key, []).append(item)
    return grouped



def normalize_series_view(view: str | None) -> str | None:
    if view is None:
        return None

    normalized = view.strip().lower()
    if normalized != "frontend":
        raise HTTPException(400, "View inválida. Use ?view=frontend.")
    return normalized



def apply_frontend_view_to_series_payload(series_list: list[dict]) -> list[dict]:
    for series_obj in series_list:
        for game_obj in series_obj.get("games", []):
            game_obj["home"] = game_obj.get("blue", {})
            game_obj["away"] = game_obj.get("red", {})
            game_obj["home_side"] = "AZUL"
            game_obj["away_side"] = "VERMELHO"
    return series_list



def fetch_series_payload(
    where_sql: str = "",
    params: tuple = (),
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict]:
    sql_params = list(params)
    pagination_sql = ""

    if limit is not None:
        pagination_sql += "\n        LIMIT %s"
        sql_params.append(limit)
        if offset is not None:
            pagination_sql += "\n        OFFSET %s"
            sql_params.append(offset)
    elif offset is not None:
        pagination_sql += "\n        OFFSET %s"
        sql_params.append(offset)

    series_rows = query(f"""
        SELECT
            s.id,
            s.match_date,
            s.match_number,
            s.stage,
            s.day,
            s.best_of,
            s.team_a_id,
            s.team_b_id,
            ta.name AS team_a,
            ta.tag AS team_a_tag,
            ta.icon_url AS team_a_icon,
            tb.name AS team_b,
            tb.tag AS team_b_tag,
            tb.icon_url AS team_b_icon
        FROM series s
        JOIN teams ta ON ta.id = s.team_a_id
        JOIN teams tb ON tb.id = s.team_b_id
        {where_sql}
        ORDER BY s.match_date DESC, s.match_number DESC{pagination_sql}
    """, tuple(sql_params))

    if not series_rows:
        return []

    series_ids = [s["id"] for s in series_rows]

    series_map: dict[int, dict] = {}
    for s in series_rows:
        sid = s["id"]
        series_map[sid] = {
            "series_id": sid,
            "match_id": sid,
            "match_number": s["match_number"],
            "date": str(s["match_date"]),
            "stage": s["stage"],
            "day": s["day"],
            "best_of": s["best_of"],
            "best_of_label": f"MD{s['best_of']}",
            "home_team_id": s["team_a_id"],
            "home_team": s["team_a"],
            "home_team_tag": s["team_a_tag"],
            "home_team_icon": s["team_a_icon"],
            "away_team_id": s["team_b_id"],
            "away_team": s["team_b"],
            "away_team_tag": s["team_b_tag"],
            "away_team_icon": s["team_b_icon"],
            "home_score": 0,
            "away_score": 0,
            # aliases inspirados no Excel
            "data": str(s["match_date"]),
            "partida": s["match_number"],
            "etapa": s["stage"],
            "dia": s["day"],
            # aliases de compatibilidade
            "team_a": s["team_a"],
            "team_a_tag": s["team_a_tag"],
            "team_a_icon": s["team_a_icon"],
            "team_b": s["team_b"],
            "team_b_tag": s["team_b_tag"],
            "team_b_icon": s["team_b_icon"],
            "team_a_wins": 0,
            "team_b_wins": 0,
            "total_games": 0,
            "games": [],
        }

    games_rows = query("""
        SELECT
            g.id AS game_id,
            g.series_id,
            g.game_number,
            g.duration_sec,
            g.winner_side,
            g.blue_team_id,
            bt.name AS blue_team,
            bt.tag AS blue_team_tag,
            g.red_team_id,
            rt.name AS red_team,
            rt.tag AS red_team_tag
        FROM games g
        JOIN teams bt ON bt.id = g.blue_team_id
        JOIN teams rt ON rt.id = g.red_team_id
        WHERE g.series_id = ANY(%s)
        ORDER BY g.series_id, g.game_number
    """, (series_ids,))

    if not games_rows:
        return [series_map[s["id"]] for s in series_rows]


@app.get("/api/series/{series_id}", tags=["Confrontos"])
def get_series(
    series_id: int,
    view: Optional[str] = Query(None, description="Use frontend para expor games[].home e games[].away"),
):
    """Detalhes completos de uma série usando o mesmo payload do endpoint unificado."""
    view = normalize_series_view(view)

    ordered = fetch_series_payload(where_sql="WHERE s.id = %s", params=(series_id,))
    if not ordered:
        raise HTTPException(404, "Série não encontrada")

    if view == "frontend":
        ordered = apply_frontend_view_to_series_payload(ordered)

    return ordered[0]


# ─── 6. JOGOS INDIVIDUAIS ──────────────────────────────────

@app.get("/api/games/{game_id}", tags=["Confrontos"])
def get_game(game_id: int):
    """Detalhes de um jogo individual."""
    game = query_one("""
        SELECT
            g.id,
            g.game_number,
            g.duration_sec,
            g.winner_side,
            g.series_id,
            s.match_date,
            s.match_number,
            s.stage,
            s.day,
            g.blue_team_id,
            bt.name AS blue_team,
            bt.tag AS blue_team_tag,
            g.red_team_id,
            rt.name AS red_team,
            rt.tag AS red_team_tag
        FROM games g
        JOIN series s ON s.id = g.series_id
        JOIN teams bt ON bt.id = g.blue_team_id
        JOIN teams rt ON rt.id = g.red_team_id
        WHERE g.id = %s
    """, (game_id,))

    if not game:
        raise HTTPException(404, "Jogo não encontrado")

    game["duration_sec"] = int(game["duration_sec"]) if game.get("duration_sec") is not None else None
    game["duration_label"] = fmt_duration(game["duration_sec"])
    game["duration"] = game["duration_label"]
    game["tempo"] = game["duration_label"]
    game["tempo_segundos"] = game["duration_sec"]
    game["lado_vencedor"] = game["winner_side"]
    game["date"] = str(game["match_date"])
    game["data"] = str(game["match_date"])
    game["partida"] = game["match_number"]
    game["etapa"] = game["stage"]
    game["dia"] = game["day"]

    players = query("""
        SELECT
            gp.team_id,
            gp.side,
            gp.role,
            p.nickname,
            c.name AS champion,
            t.name AS team,
            t.tag AS team_tag,
            gp.gold,
            gp.level,
            gp.kills,
            gp.deaths,
            gp.assists,
            gp.victory
        FROM game_players gp
        JOIN players p ON p.id = gp.player_id
        JOIN champions c ON c.id = gp.champion_id
        JOIN teams t ON t.id = gp.team_id
        WHERE gp.game_id = %s
        ORDER BY
            CASE gp.side WHEN 'AZUL' THEN 0 ELSE 1 END,
            ARRAY_POSITION(ARRAY['TOP','JG','MID','ADC','SUP'], gp.role)
    """, (game_id,))

    bans = query("""
        SELECT
            gb.team_id,
            t.name AS team,
            t.tag AS team_tag,
            c.name AS champion
        FROM game_bans gb
        JOIN teams t ON t.id = gb.team_id
        JOIN champions c ON c.id = gb.champion_id
        WHERE gb.game_id = %s
        ORDER BY gb.id
    """, (game_id,))

    for player in players:
        player["hero_name"] = player["champion"]
        player["k"] = player["kills"]
        player["d"] = player["deaths"]
        player["a"] = player["assists"]
        player["lado"] = player["side"]

    blue_players = [p for p in players if p["side"] == "AZUL"]
    red_players = [p for p in players if p["side"] == "VERMELHO"]
    blue_bans = [b["champion"] for b in bans if b["team_id"] == game["blue_team_id"]]
    red_bans = [b["champion"] for b in bans if b["team_id"] == game["red_team_id"]]

    bans_detail = []
    for ban in bans:
        ban_side = "AZUL" if ban["team_id"] == game["blue_team_id"] else "VERMELHO" if ban["team_id"] == game["red_team_id"] else None
        bans_detail.append({
            "team_id": ban["team_id"],
            "team": ban["team"],
            "team_tag": ban["team_tag"],
            "side": ban_side,
            "lado": ban_side,
            "champion": ban["champion"],
        })

    game["blue"] = {
        "team_id": game["blue_team_id"],
        "team": game["blue_team"],
        "tag": game["blue_team_tag"],
        "side": "AZUL",
        "lado": "AZUL",
        "players": blue_players,
        "bans": blue_bans,
    }
    game["red"] = {
        "team_id": game["red_team_id"],
        "team": game["red_team"],
        "tag": game["red_team_tag"],
        "side": "VERMELHO",
        "lado": "VERMELHO",
        "players": red_players,
        "bans": red_bans,
    }
    game["bans_detail"] = bans_detail
    return game


# ─── 7. UTILITÁRIOS ────────────────────────────────────────

@app.post("/api/admin/refresh-stats", tags=["Admin"], dependencies=[Depends(verify_admin)])
def refresh_stats():
    """Atualiza as materialized views. Requer header X-Api-Key."""
    conn = get_conn()
    try:
        old_autocommit = conn.autocommit
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_all_stats()")
        conn.autocommit = old_autocommit
        return {"status": "ok", "message": "Views atualizadas com sucesso."}
    finally:
        put_conn(conn)


@app.get("/api/stages", tags=["Utilitários"])
def list_stages():
    """Lista etapas disponíveis."""
    return query("SELECT DISTINCT stage FROM series ORDER BY stage")


@app.get("/api/health", tags=["Utilitários"])
def health():
    return {"status": "ok"}


# ─── OPENSERIES V1 (payloads enxutos) ─────────────────────

def _openseries_v1_player_payload(player: dict) -> dict:
    return {
        "nickname": player.get("nickname"),
        "role": player.get("role"),
        "champion": player.get("champion"),
        "level": player.get("level"),
        "kills": player.get("kills"),
        "deaths": player.get("deaths"),
        "assists": player.get("assists"),
        "gold": player.get("gold"),
    }


def _openseries_v1_series_payload(series_obj: dict) -> dict:
    return {
        "games": [
            {
                "duration": game.get("duration"),
                "winner_side": game.get("winner_side"),
                "game_number": game.get("game_number"),
                "blue": {
                    "bans": game.get("blue", {}).get("bans", []),
                    "players": [
                        _openseries_v1_player_payload(player)
                        for player in game.get("blue", {}).get("players", [])
                    ],
                },
                "red": {
                    "bans": game.get("red", {}).get("bans", []),
                    "players": [
                        _openseries_v1_player_payload(player)
                        for player in game.get("red", {}).get("players", [])
                    ],
                },
            }
            for game in series_obj.get("games", [])
        ]
    }


@app.get("/openseries/v1/agenda", tags=["OpenSeries V1"])
def get_openseries_v1_agenda(
    group: str = Query(..., min_length=1, max_length=10),
):
    """Agenda enxuta por grupo, somente com os campos usados atualmente no cliente."""
    agenda = get_agenda(bracket_group=group)
    return {
        "group": group.strip().upper(),
        "matches": [
            {
                "series_id": match.get("series_id"),
                "home_team_tag": match.get("home_team_tag"),
                "away_team_tag": match.get("away_team_tag"),
                "home_score": match.get("home_score"),
                "away_score": match.get("away_score"),
                "date": match.get("date"),
                "stage": match.get("stage"),
                "home_side": match.get("home_side"),
                "away_side": match.get("away_side"),
                "games_sides": match.get("games_sides", []),
            }
            for match in agenda.get("matches", [])
        ],
    }


@app.get("/openseries/v1/series/{series_id}", tags=["OpenSeries V1"])
def get_openseries_v1_series(series_id: int):
    """Detalhe enxuto da série, somente com os campos usados atualmente no cliente."""
    full_series = get_series(series_id)
    return _openseries_v1_series_payload(full_series)
