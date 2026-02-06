"""
main.py — FastAPI para o Dashboard Open Series 2025 (Wild Rift).

Modelo: series → games (suporta Bo1/Bo3/Bo5).

Uso:
    pip install fastapi uvicorn psycopg2-binary python-dotenv
    uvicorn main:app --reload --port 8000

Variáveis de ambiente (.env):
    SUPABASE_DB_HOST=db.xxxxxxxxxxxx.supabase.co
    SUPABASE_DB_PORT=5432
    SUPABASE_DB_NAME=postgres
    SUPABASE_DB_USER=postgres
    SUPABASE_DB_PASSWORD=sua_senha_aqui
    CORS_ORIGINS=http://localhost:3000,https://seu-site.com
    ADMIN_API_KEY=chave-secreta-aqui
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

# ─── Config ─────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("SUPABASE_DB_HOST", "localhost"),
    "port":     int(os.getenv("SUPABASE_DB_PORT", 5432)),
    "dbname":   os.getenv("SUPABASE_DB_NAME", "postgres"),
    "user":     os.getenv("SUPABASE_DB_USER", "postgres"),
    "password": os.getenv("SUPABASE_DB_PASSWORD", ""),
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
    title="Open Series 2025 — Wild Rift Dashboard API",
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


def verify_admin(x_api_key: str = Header(None)):
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(403, "API key inválida")


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
        SELECT * FROM mv_player_stats
        ORDER BY {sort_by} {order.upper()} NULLS LAST
        LIMIT %s OFFSET %s
    """, (limit, offset))


@app.get("/api/players/{player_id}", tags=["Jogadores"])
def get_player(player_id: int):
    """Detalhes de um jogador + campeões jogados."""
    row = query_one("SELECT * FROM mv_player_stats WHERE player_id = %s", (player_id,))
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
    sort_by: str = Query("games_played"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
):
    """Lista times com estatísticas."""
    if sort_by not in TEAM_SORT:
        sort_by = "games_played"
    return query(f"""
        SELECT * FROM mv_team_stats
        ORDER BY {sort_by} {order.upper()} NULLS LAST
    """)


@app.get("/api/teams/{team_id}", tags=["Times"])
def get_team(team_id: int):
    """Detalhes de um time + roster."""
    row = query_one("SELECT * FROM mv_team_stats WHERE team_id = %s", (team_id,))
    if not row:
        raise HTTPException(404, "Time não encontrado")

    roster = query("""
        SELECT
            p.id AS player_id,
            p.nickname,
            gp.role,
            COUNT(*) AS games_for_team,
            SUM(CASE WHEN gp.victory THEN 1 ELSE 0 END) AS wins,
            SUM(gp.kills) AS kills,
            SUM(gp.deaths) AS deaths,
            SUM(gp.assists) AS assists,
            ROUND((SUM(gp.kills) + SUM(gp.assists))::NUMERIC
                  / NULLIF(SUM(gp.deaths), 0), 2) AS kda
        FROM game_players gp
        JOIN players p ON p.id = gp.player_id
        WHERE gp.team_id = %s
        GROUP BY p.id, p.nickname, gp.role
        ORDER BY games_for_team DESC
    """, (team_id,))

    row["roster"] = roster
    return row


# ─── 5. CONFRONTOS (SÉRIES) ────────────────────────────────

@app.get("/api/series", tags=["Confrontos"])
def list_series(
    stage: Optional[str] = Query(None, description="Filtrar por etapa"),
    team: Optional[str] = Query(None, description="Filtrar por nome do time"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista séries com resultado resumido."""
    conditions = []
    params = []

    if stage:
        conditions.append("s.stage = %s")
        params.append(stage)
    if team:
        conditions.append("(ta.name ILIKE %s OR tb.name ILIKE %s)")
        params.extend([f"%{team}%", f"%{team}%"])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = query(f"""
        SELECT
            s.id,
            s.match_date,
            s.match_number,
            s.stage,
            s.day,
            s.best_of,
            ta.name AS team_a,
            tb.name AS team_b,
            (SELECT COUNT(DISTINCT g.id) FROM games g WHERE g.series_id = s.id) AS total_games,
            (
                SELECT COUNT(DISTINCT g.id) FROM games g
                JOIN game_players gp ON gp.game_id = g.id
                WHERE g.series_id = s.id AND gp.team_id = s.team_a_id AND gp.victory
            ) AS team_a_wins,
            (
                SELECT COUNT(DISTINCT g.id) FROM games g
                JOIN game_players gp ON gp.game_id = g.id
                WHERE g.series_id = s.id AND gp.team_id = s.team_b_id AND gp.victory
            ) AS team_b_wins
        FROM series s
        JOIN teams ta ON ta.id = s.team_a_id
        JOIN teams tb ON tb.id = s.team_b_id
        {where}
        ORDER BY s.match_date DESC, s.match_number DESC
        LIMIT %s OFFSET %s
    """, (*params, limit, offset))

    # Normalizar NULLs das subqueries
    for r in rows:
        r["team_a_wins"] = r["team_a_wins"] or 0
        r["team_b_wins"] = r["team_b_wins"] or 0

    return rows


@app.get("/api/series/{series_id}", tags=["Confrontos"])
def get_series(series_id: int):
    """Detalhes completos de uma série: jogos, picks, bans e desempenho."""
    s = query_one("""
        SELECT
            s.id, s.match_date, s.match_number, s.stage, s.day, s.best_of,
            ta.name AS team_a, tb.name AS team_b
        FROM series s
        JOIN teams ta ON ta.id = s.team_a_id
        JOIN teams tb ON tb.id = s.team_b_id
        WHERE s.id = %s
    """, (series_id,))

    if not s:
        raise HTTPException(404, "Série não encontrada")

    # Jogos da série
    games_list = query("""
        SELECT
            g.id, g.game_number, g.duration_sec, g.winner_side,
            bt.name AS blue_team, rt.name AS red_team
        FROM games g
        JOIN teams bt ON bt.id = g.blue_team_id
        JOIN teams rt ON rt.id = g.red_team_id
        WHERE g.series_id = %s
        ORDER BY g.game_number
    """, (series_id,))

    for game in games_list:
        game_id = game["id"]
        game["duration"] = fmt_duration(game.pop("duration_sec"))

        # Jogadores do jogo
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

        # Bans do jogo
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

    s["games"] = games_list
    return s


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
