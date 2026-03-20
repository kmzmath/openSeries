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
    """Formata segundos como MM:SS."""
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
            "home_seed": home["seed"],
            "away_team_id": away["team_id"],
            "away_team": away["team_name"],
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
            ta.icon_url AS team_a_icon,
            tb.name AS team_b,
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
            "home_team_icon": s["team_a_icon"],
            "away_team_id": s["team_b_id"],
            "away_team": s["team_b"],
            "away_team_icon": s["team_b_icon"],
            "home_score": 0,
            "away_score": 0,
            # aliases de compatibilidade
            "team_a": s["team_a"],
            "team_a_icon": s["team_a_icon"],
            "team_b": s["team_b"],
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
            g.red_team_id,
            rt.name AS red_team
        FROM games g
        JOIN teams bt ON bt.id = g.blue_team_id
        JOIN teams rt ON rt.id = g.red_team_id
        WHERE g.series_id = ANY(%s)
        ORDER BY g.series_id, g.game_number
    """, (series_ids,))

    if not games_rows:
        return [series_map[s["id"]] for s in series_rows]

    game_ids = [g["game_id"] for g in games_rows]

    players_rows = query("""
        SELECT
            gp.game_id,
            gp.side,
            gp.role,
            p.nickname,
            c.name AS champion,
            gp.kills,
            gp.deaths,
            gp.assists,
            gp.gold,
            gp.level
        FROM game_players gp
        JOIN players p ON p.id = gp.player_id
        JOIN champions c ON c.id = gp.champion_id
        WHERE gp.game_id = ANY(%s)
        ORDER BY
            gp.game_id,
            CASE gp.side WHEN 'AZUL' THEN 0 ELSE 1 END,
            ARRAY_POSITION(ARRAY['TOP','JG','MID','ADC','SUP'], gp.role)
    """, (game_ids,))

    bans_rows = query("""
        SELECT
            gb.game_id,
            gb.team_id,
            c.name AS champion
        FROM game_bans gb
        JOIN champions c ON c.id = gb.champion_id
        WHERE gb.game_id = ANY(%s)
        ORDER BY gb.game_id, gb.id
    """, (game_ids,))

    game_map: dict[int, dict] = {}

    for g in games_rows:
        series_obj = series_map.get(g["series_id"])
        if not series_obj:
            continue

        game_obj = {
            "game_id": g["game_id"],
            "game_number": g["game_number"],
            "winner_side": g["winner_side"],
            "duration": fmt_duration(g["duration_sec"]),
            "duration_sec": g["duration_sec"],
            "blue": {
                "team_id": g["blue_team_id"],
                "team": g["blue_team"],
                "bans": [],
                "players": [],
            },
            "red": {
                "team_id": g["red_team_id"],
                "team": g["red_team"],
                "bans": [],
                "players": [],
            },
        }

        series_obj["games"].append(game_obj)
        series_obj["total_games"] += 1

        winner_team_id = g["blue_team_id"] if g["winner_side"] == "AZUL" else g["red_team_id"]
        if winner_team_id == series_obj["home_team_id"]:
            series_obj["home_score"] += 1
            series_obj["team_a_wins"] += 1
        elif winner_team_id == series_obj["away_team_id"]:
            series_obj["away_score"] += 1
            series_obj["team_b_wins"] += 1

        game_map[g["game_id"]] = game_obj

    for b in bans_rows:
        game_obj = game_map.get(b["game_id"])
        if not game_obj:
            continue
        if b["team_id"] == game_obj["blue"]["team_id"]:
            game_obj["blue"]["bans"].append(b["champion"])
        elif b["team_id"] == game_obj["red"]["team_id"]:
            game_obj["red"]["bans"].append(b["champion"])

    for pr in players_rows:
        game_obj = game_map.get(pr["game_id"])
        if not game_obj:
            continue

        payload = {
            "nickname": pr["nickname"],
            "role": pr["role"],
            "champion": pr["champion"],
            "kills": pr["kills"],
            "deaths": pr["deaths"],
            "assists": pr["assists"],
            "gold": pr["gold"],
            "level": pr["level"],
        }

        if pr["side"] == "AZUL":
            game_obj["blue"]["players"].append(payload)
        else:
            game_obj["red"]["players"].append(payload)

    return [series_map[s["id"]] for s in series_rows]


# ─── Whitelist de colunas para ORDER BY (previne SQL injection) ──

CHAMPION_SORT = {
    "champion_name", "picks", "bans", "pick_rate", "ban_rate",
    "wins", "losses", "win_rate", "total_kills", "total_deaths",
    "total_assists", "kda", "avg_gold",
}
PLAYER_SORT = {
    "nickname", "games_played", "wins", "losses", "win_rate",
    "total_kills", "total_deaths", "total_assists", "kda", "avg_gold",
}
TEAM_SORT = {
    "team_name", "games_played", "wins", "losses", "win_rate",
    "avg_kills", "avg_deaths", "avg_assists", "avg_gold",
    "bracket_group", "status", "seed", "tie_breaker",
}



# ═══════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════


# ─── 1. VISÃO GERAL ────────────────────────────────────────

@app.get("/api/overview", tags=["Visão Geral"])
def get_overview():
    """
    Estatísticas gerais do campeonato.
    - total_games: jogos individuais (não séries)
    - total_series: séries (partidas no sentido do Excel)
    - avg_duration: duração média em MM:SS
    """
    row = query_one("""
        SELECT
            (SELECT COUNT(*) FROM games)  AS total_games,
            (SELECT COUNT(*) FROM series) AS total_series,
            (SELECT COUNT(*) FROM players) AS unique_players,
            (SELECT COUNT(DISTINCT champion_id) FROM game_players) AS unique_champions,
            (SELECT ROUND(AVG(duration_sec)) FROM games) AS avg_duration_sec,
            (
                SELECT ROUND(
                    SUM(CASE WHEN winner_side = 'AZUL' THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0), 1
                ) FROM games
            ) AS blue_winrate,
            (
                SELECT ROUND(
                    SUM(CASE WHEN winner_side = 'VERMELHO' THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0), 1
                ) FROM games
            ) AS red_winrate
    """)
    if row:
        row["avg_duration"] = fmt_duration(row.pop("avg_duration_sec"))
    return row


# ─── 2. CAMPEÕES ───────────────────────────────────────────

@app.get("/api/champions", tags=["Campeões"])
def list_champions(
    sort_by: str = Query("picks"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista campeões com pick/ban/win/KDA/gold."""
    if sort_by not in CHAMPION_SORT:
        sort_by = "picks"
    return query(f"""
        SELECT * FROM mv_champion_stats
        ORDER BY {sort_by} {order.upper()} NULLS LAST
        LIMIT %s OFFSET %s
    """, (limit, offset))


@app.get("/api/champions/{champion_id}", tags=["Campeões"])
def get_champion(champion_id: int):
    """Detalhes de um campeão."""
    row = query_one("SELECT * FROM mv_champion_stats WHERE champion_id = %s", (champion_id,))
    if not row:
        raise HTTPException(404, "Campeão não encontrado")
    return row


# ─── 3. JOGADORES ──────────────────────────────────────────

@app.get("/api/players", tags=["Jogadores"])
def list_players(
    sort_by: str = Query("games_played"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista jogadores com estatísticas."""
    if sort_by not in PLAYER_SORT:
        sort_by = "games_played"
    return query(f"""
        SELECT
        mps.*,
        p.team_id,
        t.name AS team_name,
        t.icon_url AS team_icon_url
        FROM mv_player_stats mps
        JOIN players p ON p.id = mps.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        ORDER BY mps.{sort_by} {order.upper()} NULLS LAST
        LIMIT %s OFFSET %s
    """, (limit, offset))




@app.get("/api/players/{player_id}", tags=["Jogadores"])
def get_player(player_id: int):
    """Detalhes de um jogador + campeões jogados."""
    row = query_one("""
        SELECT
        mps.*,
        p.team_id,
        t.name AS team_name,
        t.icon_url AS team_icon_url
        FROM mv_player_stats mps
        JOIN players p ON p.id = mps.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        WHERE mps.player_id = %s
    """, (player_id,))
    if not row:
        raise HTTPException(404, "Jogador não encontrado")

    champs = query("""
        SELECT
            c.name AS champion_name,
            COUNT(*) AS times_played,
            SUM(CASE WHEN gp.victory THEN 1 ELSE 0 END) AS wins,
            SUM(gp.kills) AS kills,
            SUM(gp.deaths) AS deaths,
            SUM(gp.assists) AS assists,
            ROUND((SUM(gp.kills) + SUM(gp.assists))::NUMERIC
                  / NULLIF(SUM(gp.deaths), 0), 2) AS kda
        FROM game_players gp
        JOIN champions c ON c.id = gp.champion_id
        WHERE gp.player_id = %s
        GROUP BY c.name
        ORDER BY times_played DESC
    """, (player_id,))

    row["champions_played"] = champs
    return row


# ─── 4. TIMES ──────────────────────────────────────────────

@app.get("/api/teams", tags=["Times"])
def list_teams(
    group_: Optional[str] = Query(None, alias="group"),
    status: Optional[str] = Query(
        None,
        pattern="^(DESISTENTE|PENDENTE|APROVADA|DESCLASSIFICADA|REPROVADA)$",
    ),
    include_lineup: bool = Query(False),
    sort_by: str = Query("games_played"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
):
    if sort_by not in TEAM_SORT:
        sort_by = "games_played"

    conditions = []
    params = []

    if group_:
        conditions.append("mts.bracket_group = %s")
        params.append(group_)

    if status:
        conditions.append("mts.status = %s")
        params.append(status)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    if include_lineup:
        sql = f"""
            SELECT
              mts.*,
              gs.starts_at AS group_start_at,
              COALESCE(l.lineup, '[]'::json) AS lineup
            FROM mv_team_stats mts
            LEFT JOIN public.group_schedules gs
              ON gs.bracket_group = mts.bracket_group
            LEFT JOIN LATERAL (
              SELECT json_agg(
                       json_build_object('player_id', p.id, 'nickname', p.nickname)
                       ORDER BY p.nickname
                     ) AS lineup
              FROM players p
              WHERE p.team_id = mts.team_id
            ) l ON TRUE
            {where}
            ORDER BY mts.{sort_by} {order.upper()} NULLS LAST
        """
    else:
        sql = f"""
            SELECT
              mts.*,
              gs.starts_at AS group_start_at
            FROM mv_team_stats mts
            LEFT JOIN public.group_schedules gs
              ON gs.bracket_group = mts.bracket_group
            {where}
            ORDER BY mts.{sort_by} {order.upper()} NULLS LAST
        """

    teams = query(sql, tuple(params))
    for team in teams:
        raw_start = team.get("group_start_at")
        team["group_start_label"] = fmt_datetime_br(raw_start)
        team["group_start_at"] = raw_start.isoformat() if raw_start else None

    group_sql = """
        SELECT
            gs.bracket_group AS group_code,
            gs.starts_at
        FROM public.group_schedules gs
        {where}
        ORDER BY gs.starts_at ASC, gs.bracket_group ASC
    """
    group_where = "WHERE gs.bracket_group = %s" if group_ else ""
    groups = query(group_sql.format(where=group_where), ((group_,) if group_ else ()))

    for group in groups:
        raw_start = group.get("starts_at")
        group["group_name"] = f"Grupo {group['group_code']}"
        group["start_label"] = fmt_datetime_br(raw_start)
        group["starts_at"] = raw_start.isoformat() if raw_start else None

    return {
        "groups": groups,
        "teams": teams,
    }

@app.get("/api/teams/{team_id}", tags=["Times"])
def get_team(team_id: int):
    """Detalhes de um time + roster."""
    row = query_one("""
        SELECT
            mts.*,
            gs.starts_at AS group_start_at
        FROM mv_team_stats mts
        LEFT JOIN public.group_schedules gs
          ON gs.bracket_group = mts.bracket_group
        WHERE mts.team_id = %s
    """, (team_id,))
    if not row:
        raise HTTPException(404, "Time não encontrado")

    raw_start = row.get("group_start_at")
    row["group_start_label"] = fmt_datetime_br(raw_start)
    row["group_start_at"] = raw_start.isoformat() if raw_start else None

    roster = query("""
        WITH agg AS (
            SELECT
                gp.player_id,
                COUNT(*) AS games_for_team,
                SUM(CASE WHEN gp.victory THEN 1 ELSE 0 END) AS wins,
                SUM(gp.kills) AS kills,
                SUM(gp.deaths) AS deaths,
                SUM(gp.assists) AS assists,
                ROUND((SUM(gp.kills) + SUM(gp.assists))::NUMERIC
                    / NULLIF(SUM(gp.deaths), 0), 2) AS kda
            FROM game_players gp
            WHERE gp.team_id = %s
            GROUP BY gp.player_id
        ),
        role_counts AS (
            SELECT
                gp.player_id,
                gp.role,
                COUNT(*) AS role_games,
                ROW_NUMBER() OVER (PARTITION BY gp.player_id ORDER BY COUNT(*) DESC) AS rn
            FROM game_players gp
            WHERE gp.team_id = %s
            GROUP BY gp.player_id, gp.role
        )
        SELECT
            p.id AS player_id,
            p.nickname,
            rc.role AS main_role,
            COALESCE(a.games_for_team, 0) AS games_for_team,
            COALESCE(a.wins, 0) AS wins,
            COALESCE(a.kills, 0) AS kills,
            COALESCE(a.deaths, 0) AS deaths,
            COALESCE(a.assists, 0) AS assists,
            COALESCE(a.kda, 0) AS kda
        FROM players p
        LEFT JOIN agg a ON a.player_id = p.id
        LEFT JOIN role_counts rc ON rc.player_id = p.id AND rc.rn = 1
        WHERE p.team_id = %s
        ORDER BY games_for_team DESC, p.nickname
    """, (team_id, team_id, team_id))
    row["lineup"] = roster
    return row



@app.get("/api/agenda", tags=["Times"])
def get_agenda(
    bracket_group: str = Query(..., min_length=1, max_length=10),
):
    """Retorna a agenda fixa do grupo e, quando existir série cadastrada, também o placar."""
    bracket_group = bracket_group.strip().upper()

    teams = query("""
        SELECT
            t.id AS team_id,
            t.name AS team_name,
            t.icon_url,
            t.bracket_group,
            t.seed,
            t.tie_breaker,
            gs.starts_at AS group_start_at
        FROM public.teams t
        LEFT JOIN public.group_schedules gs
          ON gs.bracket_group = t.bracket_group
        WHERE t.bracket_group = %s
        ORDER BY t.seed ASC NULLS LAST, t.tie_breaker ASC NULLS LAST, t.name ASC
    """, (bracket_group,))

    if not teams:
        raise HTTPException(404, f"Nenhum time encontrado para o grupo {bracket_group}")

    group_start_at = teams[0].get("group_start_at")
    for team in teams:
        team.pop("group_start_at", None)

    agenda = build_group_agenda(teams, bracket_group, starts_at=group_start_at)

    series_rows = query("""
        SELECT
            s.id AS series_id,
            s.match_date,
            s.match_number,
            s.stage,
            s.day,
            s.best_of,
            s.team_a_id,
            s.team_b_id,
            COALESCE(SUM(
                CASE
                    WHEN g.winner_side = 'AZUL' AND g.blue_team_id = s.team_a_id THEN 1
                    WHEN g.winner_side = 'VERMELHO' AND g.red_team_id = s.team_a_id THEN 1
                    ELSE 0
                END
            ), 0) AS team_a_score,
            COALESCE(SUM(
                CASE
                    WHEN g.winner_side = 'AZUL' AND g.blue_team_id = s.team_b_id THEN 1
                    WHEN g.winner_side = 'VERMELHO' AND g.red_team_id = s.team_b_id THEN 1
                    ELSE 0
                END
            ), 0) AS team_b_score
        FROM series s
        JOIN teams ta ON ta.id = s.team_a_id
        JOIN teams tb ON tb.id = s.team_b_id
        LEFT JOIN games g ON g.series_id = s.id
        WHERE ta.bracket_group = %s
          AND tb.bracket_group = %s
        GROUP BY
            s.id,
            s.match_date,
            s.match_number,
            s.stage,
            s.day,
            s.best_of,
            s.team_a_id,
            s.team_b_id
        ORDER BY s.match_date, s.match_number
    """, (bracket_group, bracket_group))

    series_lookup: dict[tuple[int, int], dict] = {}
    for row in series_rows:
        forward = {
            "series_id": row["series_id"],
            "match_id": row["series_id"],
            "match_number": row["match_number"],
            "date": str(row["match_date"]),
            "stage": row["stage"],
            "day": row["day"],
            "best_of": row["best_of"],
            "best_of_label": f"MD{row['best_of']}",
            "home_score": row["team_a_score"],
            "away_score": row["team_b_score"],
        }
        reverse = {
            "series_id": row["series_id"],
            "match_id": row["series_id"],
            "match_number": row["match_number"],
            "date": str(row["match_date"]),
            "stage": row["stage"],
            "day": row["day"],
            "best_of": row["best_of"],
            "best_of_label": f"MD{row['best_of']}",
            "home_score": row["team_b_score"],
            "away_score": row["team_a_score"],
        }
        series_lookup[(row["team_a_id"], row["team_b_id"])] = forward
        series_lookup[(row["team_b_id"], row["team_a_id"])] = reverse

    for match in agenda["matches"]:
        series_data = series_lookup.get((match["home_team_id"], match["away_team_id"]))
        if series_data:
            match.update(series_data)
        else:
            match.update({
                "series_id": None,
                "match_id": None,
                "match_number": None,
                "date": None,
                "stage": None,
                "day": None,
                "best_of": None,
                "best_of_label": None,
                "home_score": None,
                "away_score": None,
            })

    return agenda


# ─── 5. CONFRONTOS (SÉRIES) ────────────────────────────────

@app.get("/api/series", tags=["Confrontos"])
def list_series(
    stage: Optional[str] = Query(None, description="Filtrar por etapa"),
    team: Optional[str] = Query(None, description="Filtrar por nome do time"),
    best_of: Optional[int] = Query(None, ge=1, le=5, description="Filtrar MD1/MD3/MD5"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """
    Endpoint unificado de confrontos.
    Retorna todas as séries com metadados da partida, placar agregado,
    jogos, bans e estatísticas individuais dos jogadores.
    """
    conditions = []
    params = []

    if stage:
        conditions.append("s.stage = %s")
        params.append(stage)
    if team:
        conditions.append("(ta.name ILIKE %s OR tb.name ILIKE %s)")
        params.extend([f"%{team}%", f"%{team}%"])
    if best_of is not None:
        conditions.append("s.best_of = %s")
        params.append(best_of)

    where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    ordered = fetch_series_payload(where_sql=where_sql, params=tuple(params), limit=limit, offset=offset)

    return {
        "series": ordered,
        "by_best_of": group_series_by_best_of(ordered),
    }

    game_ids = [g["game_id"] for g in games_rows]

    # 3) Players de todos os jogos (ordenados por side e role)
    players_rows = query("""
        SELECT
            gp.game_id,
            gp.side,
            gp.role,
            p.nickname,
            c.name AS champion,
            gp.kills,
            gp.deaths,
            gp.assists,
            gp.gold
        FROM game_players gp
        JOIN players p ON p.id = gp.player_id
        JOIN champions c ON c.id = gp.champion_id
        WHERE gp.game_id = ANY(%s)
        ORDER BY
            gp.game_id,
            CASE gp.side WHEN 'AZUL' THEN 0 ELSE 1 END,
            ARRAY_POSITION(ARRAY['TOP','JG','MID','ADC','SUP'], gp.role)
    """, (game_ids,))

    # 4) Bans de todos os jogos (ORDER BY p/ ordem estável)
    bans_rows = query("""
        SELECT
            gb.game_id,
            gb.team_id,
            c.name AS champion
        FROM game_bans gb
        JOIN champions c ON c.id = gb.champion_id
        WHERE gb.game_id = ANY(%s)
        ORDER BY gb.game_id, gb.id
    """, (game_ids,))

    # Montagem: mapa de game_id -> objeto game
    game_map: dict[int, dict] = {}

    for g in games_rows:
        sid = g["series_id"]
        s = series_map.get(sid)
        if not s:
            continue

        game_obj = {
            "game_id": g["game_id"],
            "game_number": g["game_number"],
            "winner_side": g["winner_side"],
            "duration": fmt_duration(g["duration_sec"]),
            "blue": {
                "team": g["blue_team"],
                "bans": [],
                "players": [],
            },
            "red": {
                "team": g["red_team"],
                "bans": [],
                "players": [],
            },
            # internos p/ atribuir bans e wins
            "_blue_team_id": g["blue_team_id"],
            "_red_team_id": g["red_team_id"],
        }

        s["games"].append(game_obj)
        s["total_games"] += 1

        # wins por team_a/team_b (por time vencedor)
        winner_team_id = g["blue_team_id"] if g["winner_side"] == "AZUL" else g["red_team_id"]
        if winner_team_id == s["_team_a_id"]:
            s["team_a_wins"] += 1
        elif winner_team_id == s["_team_b_id"]:
            s["team_b_wins"] += 1

        game_map[g["game_id"]] = game_obj

    # Atribuir bans para blue/red via team_id
    for b in bans_rows:
        game_obj = game_map.get(b["game_id"])
        if not game_obj:
            continue
        if b["team_id"] == game_obj["_blue_team_id"]:
            game_obj["blue"]["bans"].append(b["champion"])
        elif b["team_id"] == game_obj["_red_team_id"]:
            game_obj["red"]["bans"].append(b["champion"])

    # Atribuir players para blue/red via side
    for pr in players_rows:
        game_obj = game_map.get(pr["game_id"])
        if not game_obj:
            continue
        payload = {
            "nickname": pr["nickname"],
            "champion": pr["champion"],
            "kills": pr["kills"],
            "deaths": pr["deaths"],
            "assists": pr["assists"],
            "gold": pr["gold"],
        }
        if pr["side"] == "AZUL":
            game_obj["blue"]["players"].append(payload)
        else:
            game_obj["red"]["players"].append(payload)

    # Remover campos internos antes de retornar
    ordered = [series_map[s["id"]] for s in series_rows]
    for s in ordered:
        s.pop("_team_a_id", None)
        s.pop("_team_b_id", None)
        for g in s["games"]:
            g.pop("_blue_team_id", None)
            g.pop("_red_team_id", None)

    return ordered

@app.get("/api/series/{series_id}", tags=["Confrontos"])
def get_series(series_id: int):
    """Detalhes completos de uma série usando o mesmo payload do endpoint unificado."""
    ordered = fetch_series_payload(where_sql="WHERE s.id = %s", params=(series_id,))
    if not ordered:
        raise HTTPException(404, "Série não encontrada")
    return ordered[0]


# ─── 6. JOGOS INDIVIDUAIS ──────────────────────────────────

@app.get("/api/games/{game_id}", tags=["Confrontos"])
def get_game(game_id: int):
    """Detalhes de um jogo individual."""
    game = query_one("""
        SELECT
            g.id, g.game_number, g.duration_sec, g.winner_side,
            g.series_id,
            bt.name AS blue_team, rt.name AS red_team
        FROM games g
        JOIN teams bt ON bt.id = g.blue_team_id
        JOIN teams rt ON rt.id = g.red_team_id
        WHERE g.id = %s
    """, (game_id,))

    if not game:
        raise HTTPException(404, "Jogo não encontrado")

    game["duration"] = fmt_duration(game.pop("duration_sec"))

    players = query("""
        SELECT
            gp.side, gp.role,
            p.nickname,
            c.name AS champion,
            t.name AS team,
            gp.gold, gp.level,
            gp.kills, gp.deaths, gp.assists,
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
        SELECT t.name AS team, c.name AS champion
        FROM game_bans gb
        JOIN teams t ON t.id = gb.team_id
        JOIN champions c ON c.id = gb.champion_id
        WHERE gb.game_id = %s
    """, (game_id,))

    blue_players = [p for p in players if p["side"] == "AZUL"]
    red_players = [p for p in players if p["side"] == "VERMELHO"]
    blue_bans = [b["champion"] for b in bans if b["team"] == game["blue_team"]]
    red_bans = [b["champion"] for b in bans if b["team"] == game["red_team"]]

    game["blue"] = {"team": game["blue_team"], "players": blue_players, "bans": blue_bans}
    game["red"] = {"team": game["red_team"], "players": red_players, "bans": red_bans}
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
