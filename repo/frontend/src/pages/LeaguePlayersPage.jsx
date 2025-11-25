// src/pages/LeaguePlayersPage.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import { useParams, Link } from "react-router-dom";
import "./LeaguePlayersPage.css";

const API_BASE = "http://127.0.0.1:8000";

export default function LeaguePlayersPage() {
  const { league } = useParams();

  // datos base
  const [players, setPlayers] = useState([]);
  const [leagues, setLeagues] = useState([]);
  const [seasons, setSeasons] = useState([]);
  const [selectedSeason, setSelectedSeason] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // vistas: "players" | "score"
  const [view, setView] = useState("players");

  // score (cacheado por temporada)
  const [scoreBySeason, setScoreBySeason] = useState({}); // { [season]: rows }
  const [loadingScore, setLoadingScore] = useState(false);
  const [errorScore, setErrorScore] = useState("");

  // filtro por posiciones
  const [selectedPositions, setSelectedPositions] = useState([]);

  // paginación
  const [page, setPage] = useState(0);
  const rowsPerPage = 50;

  // AbortControllers
  const leaguesAbortRef = useRef(null);
  const seasonsAbortRef = useRef(null);
  const playersAbortRef = useRef(null);
  const scoreAbortRef = useRef(null);

  // columnas numéricas
  const numericCols = useMemo(
    () => [
      "minutesPlayed",
      "games",
      "goals",
      "assists",
      "xG_per90",
      "xA_per90",
      "xGChain_per90",
      "xGBuildup_per90",
      "performance_score",
      "penalty_score",
      "adjusted_score",
      "market_value",
    ],
    []
  );

  const scoreColsPreferred = useMemo(
    () => [
      "player_name",
      "team_name",
      "season",
      "liga",
      "position",
      "performance_score",
      "penalty_score",
      "adjusted_score",
      "market_value"
    ],
    []
  );

  // ======= 1) Ligas =======
  useEffect(() => {
    setError("");
    if (leaguesAbortRef.current) leaguesAbortRef.current.abort();
    const ac = new AbortController();
    leaguesAbortRef.current = ac;

    (async () => {
      try {
        const res = await fetch(`${API_BASE}/leagues`, { signal: ac.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setLeagues(await res.json());
      } catch (e) {
        if (e.name !== "AbortError")
          setError(`No se pudieron cargar las ligas: ${e.message}`);
      }
    })();

    return () => ac.abort();
  }, []);

  // ======= 2) Temporadas disponibles por liga =======
  useEffect(() => {
    setSeasons([]);
    setSelectedSeason("");
    setScoreBySeason({});
    setErrorScore("");
    setSelectedPositions([]);
    setPage(0);

    if (!league) return;
    if (seasonsAbortRef.current) seasonsAbortRef.current.abort();
    const ac = new AbortController();
    seasonsAbortRef.current = ac;

    (async () => {
      try {
        const res = await fetch(
          `${API_BASE}/league/${encodeURIComponent(league)}/season_summary`,
          { signal: ac.signal }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const uniqueSeasons = [...new Set(data.map((r) => r.season))];
        setSeasons(uniqueSeasons);
        if (uniqueSeasons.length) setSelectedSeason(uniqueSeasons[0]);
      } catch (e) {
        if (e.name !== "AbortError")
          setError(`No se pudieron cargar las temporadas: ${e.message}`);
      }
    })();

    return () => ac.abort();
  }, [league]);

  // ======= 3) Jugadores (tabla base) =======
  useEffect(() => {
    if (!league || !selectedSeason) return;

    setLoading(true);
    setError("");

    if (playersAbortRef.current) playersAbortRef.current.abort();
    const ac = new AbortController();
    playersAbortRef.current = ac;

    (async () => {
      try {
        let url = `${API_BASE}/players?league=${encodeURIComponent(
          league
        )}&limit=300`;
        if (selectedSeason) url += `&season=${encodeURIComponent(selectedSeason)}`;
        const res = await fetch(url, { signal: ac.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setPlayers(await res.json());
      } catch (e) {
        if (e.name !== "AbortError")
          setError(`No se pudieron cargar los jugadores: ${e.message}`);
      } finally {
        setLoading(false);
      }
    })();

    return () => ac.abort();
  }, [league, selectedSeason]);

  // ======= Score por temporada (lazy + cache) =======
  async function ensureScoreLoaded(season) {
    if (!season) return;
    if (scoreBySeason[season] || loadingScore) return;
    setLoadingScore(true);
    setErrorScore("");

    if (scoreAbortRef.current) scoreAbortRef.current.abort();
    const ac = new AbortController();
    scoreAbortRef.current = ac;

    try {
      let url = `${API_BASE}/players_scores?league=${encodeURIComponent(
        league
      )}&limit=300&sort_by=-adjusted_score`;
      url += `&season=${encodeURIComponent(season)}`;

      const res = await fetch(url, { signal: ac.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setScoreBySeason((prev) => ({ ...prev, [season]: data }));
    } catch (e) {
      if (e.name !== "AbortError")
        setErrorScore(`No se pudo cargar el score: ${e.message}`);
    } finally {
      setLoadingScore(false);
    }
  }

  // Auto-cargar score al entrar en la vista score o al cambiar season
  useEffect(() => {
    if (view === "score" && selectedSeason) {
      ensureScoreLoaded(selectedSeason);
    }
  }, [view, selectedSeason]);

  // Reset filtros y página al cambiar season / view / filtro
  useEffect(() => {
    setSelectedPositions([]);
    setPage(0);
  }, [selectedSeason]);

  useEffect(() => {
    setPage(0);
  }, [view]);

  // columnas
  const playerCols = useMemo(() => Object.keys(players[0] || {}), [players]);
  const scoreCols = useMemo(() => {
    const sample = scoreBySeason[selectedSeason]?.[0] || players[0] || {};
    return scoreColsPreferred.filter((c) => c in sample);
  }, [players, scoreBySeason, selectedSeason, scoreColsPreferred]);

  // Posiciones disponibles (jugadores + score de la season)
  const availablePositions = useMemo(() => {
    const vals = new Set();
    (players || []).forEach((p) => p?.position && vals.add(String(p.position)));
    (scoreBySeason[selectedSeason] || []).forEach(
      (p) => p?.position && vals.add(String(p.position))
    );
    return Array.from(vals).sort();
  }, [players, scoreBySeason, selectedSeason]);

  // helpers de formato
  const fmt = (val, key) => {
    if (val == null) return "";
    if (typeof val === "number") {
      const isPct90 = String(key || "").includes("per90");
      const isScore = ["performance_score", "penalty_score", "adjusted_score"].includes(
        key || ""
      );
      const isMoney = key === "market_value_eur";
      if (isMoney) return Intl.NumberFormat("es-ES").format(Math.round(val));
      if (isScore) return Intl.NumberFormat("es-ES", { maximumFractionDigits: 2 }).format(val);
      if (isPct90) return Intl.NumberFormat("es-ES", { maximumFractionDigits: 3 }).format(val);
      return Intl.NumberFormat("es-ES").format(val);
    }
    return String(val);
  };

  // Filtrado por posiciones
  const filteredPlayers = useMemo(() => {
    if (!selectedPositions.length) return players;
    return players.filter((p) => selectedPositions.includes(String(p.position)));
  }, [players, selectedPositions]);

  const filteredScore = useMemo(() => {
    const rows = scoreBySeason[selectedSeason] || [];
    if (!selectedPositions.length) return rows;
    return rows.filter((p) => selectedPositions.includes(String(p.position)));
  }, [scoreBySeason, selectedSeason, selectedPositions]);

  // datos visibles según página
  const visiblePlayers = useMemo(
    () => filteredPlayers.slice(page * rowsPerPage, (page + 1) * rowsPerPage),
    [filteredPlayers, page]
  );
  const visibleScore = useMemo(
    () => filteredScore.slice(page * rowsPerPage, (page + 1) * rowsPerPage),
    [filteredScore, page]
  );

  // paginación: totales y deshabilitados
  const totalPlayers = filteredPlayers.length;
  const totalScore = filteredScore.length;
  const isPrevDisabled = page === 0;
  const isNextDisabled =
    view === "players"
      ? (page + 1) * rowsPerPage >= totalPlayers
      : (page + 1) * rowsPerPage >= totalScore;

  // handlers
  const toggleScore = async () => {
    const next = view === "score" ? "players" : "score";
    setView(next);
    if (next === "score") await ensureScoreLoaded(selectedSeason);
  };

  function togglePosition(pos) {
    setSelectedPositions((prev) =>
      prev.includes(pos) ? prev.filter((p) => p !== pos) : [...prev, pos]
    );
    setPage(0);
  }
  function clearPositions() {
    setSelectedPositions([]);
    setPage(0);
  }

  // estados de carga
  if (view === "players" && (loading || !players.length) && !error) {
    return <h2 style={{ padding: "2rem" }}>Cargando jugadores...</h2>;
  }
  if (view === "score" && loadingScore && !(scoreBySeason[selectedSeason]?.length) && !errorScore) {
    return <h2 style={{ padding: "2rem" }}>Cargando score...</h2>;
  }

  return (
    <div className="page-league">
      <Link to="/" className="back-link">
        ← Volver a ligas
      </Link>
      <h1>Jugadores – {decodeURIComponent(league || "")}</h1>

      {/* Botones de ligas */}
      <div className="league-buttons-row">
        {leagues.map((lg) => (
          <Link
            key={lg}
            to={`/league/${encodeURIComponent(lg)}`}
            className={`league-switch-button ${lg === league ? "active" : ""}`}
          >
            {lg}
          </Link>
        ))}
      </div>

      {/* Toolbar */}
      <div className="toolbar-row">
        <button className="toggle-table-btn" onClick={toggleScore}>
          {view === "score" ? "Ver tabla de jugadores" : "score"}
        </button>
      </div>

      {(error || errorScore) && (
        <div className="error-box" role="alert" style={{ marginTop: "1rem" }}>
          {error || errorScore}
        </div>
      )}

      <div className="content-layout">
        {/* Sidebar temporadas + posiciones */}
        <aside className="sidebar">
          <h2 className="sidebar-title">Temporadas</h2>
          <div className="seasons-column">
            {seasons.map((season) => (
              <button
                key={season}
                onClick={() => {
                  setSelectedSeason(season);
                  setPage(0);
                  if (view === "score") ensureScoreLoaded(season);
                }}
                className={`season-button ${season === selectedSeason ? "active" : ""}`}
              >
                {season}
              </button>
            ))}
          </div>

          {availablePositions.length > 0 && (
            <>
              <h2 className="sidebar-title" style={{ marginTop: "1rem" }}>
                Posiciones
              </h2>
              <div className="positions-column">
                {availablePositions.map((pos) => {
                  const active = selectedPositions.includes(pos);
                  return (
                    <button
                      key={pos}
                      onClick={() => togglePosition(pos)}
                      className={`team-button ${active ? "active" : ""}`}
                      title={active ? "Quitar filtro" : "Añadir filtro"}
                    >
                      {pos}
                    </button>
                  );
                })}
                {selectedPositions.length > 0 && (
                  <button
                    onClick={clearPositions}
                    className="team-button"
                    style={{ marginTop: ".4rem", backgroundColor: "#475569" }}
                  >
                    Limpiar filtros
                  </button>
                )}
              </div>
            </>
          )}
        </aside>

        {/* Tabla según vista */}
        <div className="table-wrapper">
          {view === "players" && visiblePlayers.length > 0 && (
            <>
              <table className="league-table">
                <thead>
                  <tr>
                    {playerCols.map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {visiblePlayers.map((p, i) => (
                    <tr key={`${p.id || p.player_id || p.player_name || i}-${i}`}>
                      {playerCols.map((c) => (
                        <td key={c} className={numericCols.includes(c) ? "col-number" : ""}>
                          {fmt(p[c], c)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="pagination-controls">
                <button disabled={isPrevDisabled} onClick={() => setPage((p) => p - 1)}>
                  ← Anterior
                </button>
                <span style={{ margin: "0 1rem" }}>
                  Página {page + 1} de {Math.max(1, Math.ceil(totalPlayers / rowsPerPage))}
                </span>
                <button disabled={isNextDisabled} onClick={() => setPage((p) => p + 1)}>
                  Siguiente →
                </button>
              </div>
            </>
          )}

          {view === "score" && (
            <>
              <table className="league-table">
                <thead>
                  <tr>
                    {scoreCols.map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {visibleScore.map((p, i) => (
                    <tr key={`${p.player_name || p.id || i}-${i}`}>
                      {scoreCols.map((c) => (
                        <td key={c} className={numericCols.includes(c) ? "col-number" : ""}>
                          {fmt(p[c], c)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="pagination-controls">
                <button disabled={isPrevDisabled} onClick={() => setPage((p) => p - 1)}>
                  ← Anterior
                </button>
                <span style={{ margin: "0 1rem" }}>
                  Página {page + 1} de {Math.max(1, Math.ceil(totalScore / rowsPerPage))}
                </span>
                <button disabled={isNextDisabled} onClick={() => setPage((p) => p + 1)}>
                  Siguiente →
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
