// src/pages/LeaguesPage.jsx
import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import "./LeaguesPage.css";

const API_BASE = "http://127.0.0.1:8000";

export default function LeaguesPage() {
  const [leagues, setLeagues] = useState([]);
  const [loadingLeagues, setLoadingLeagues] = useState(true);
  const [errorLeagues, setErrorLeagues] = useState("");

  const [summaries, setSummaries] = useState([]); // filas: { liga, season, num_players, total_minutes, total_goals, total_assists }
  const [loadingSummaries, setLoadingSummaries] = useState(false);
  const [errorSummaries, setErrorSummaries] = useState("");

  const leaguesAbortRef = useRef(null);
  const summariesAbortRef = useRef(null);

  // ===== 1) Cargar ligas =====
  useEffect(() => {
    if (leaguesAbortRef.current) leaguesAbortRef.current.abort();
    const ac = new AbortController();
    leaguesAbortRef.current = ac;

    async function loadLeagues() {
      try {
        setLoadingLeagues(true);
        setErrorLeagues("");
        const res = await fetch(`${API_BASE}/leagues`, { signal: ac.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setLeagues(data);
      } catch (err) {
        if (err.name !== "AbortError") setErrorLeagues(`No se pudieron cargar las ligas: ${err.message}`);
      } finally {
        setLoadingLeagues(false);
      }
    }

    loadLeagues();
    return () => ac.abort();
  }, []);

  // ===== 2) Cargar resúmenes para todas las ligas =====
  useEffect(() => {
    if (!leagues.length) {
      setSummaries([]);
      return;
    }

    if (summariesAbortRef.current) summariesAbortRef.current.abort();
    const ac = new AbortController();
    summariesAbortRef.current = ac;

    async function loadAllSummaries() {
      try {
        setLoadingSummaries(true);
        setErrorSummaries("");

        const requests = leagues.map(async (lg) => {
          const url = `${API_BASE}/league/${encodeURIComponent(lg)}/season_summary`;
          const res = await fetch(url, { signal: ac.signal });
          if (!res.ok) throw new Error(`HTTP ${res.status} (${lg})`);
          const rows = await res.json();
          return rows.map((r) => ({ liga: lg, ...r }));
        });

        const results = await Promise.all(requests);
        setSummaries(results.flat());
      } catch (err) {
        if (err.name !== "AbortError") setErrorSummaries(`No se pudieron cargar los resúmenes: ${err.message}`);
      } finally {
        setLoadingSummaries(false);
      }
    }

    loadAllSummaries();
    return () => ac.abort();
  }, [leagues]);

  const columns = useMemo(
    () => ["liga", "season", "num_players", "total_minutes", "total_goals", "total_assists"],
    []
  );

  const isNumber = (k) => ["num_players", "total_minutes", "total_goals", "total_assists"].includes(k);
  const fmt = (val) => (typeof val === "number" ? Intl.NumberFormat("es-ES").format(val) : String(val ?? ""));

  return (
    <div className="page-container">
      <h1 className="title">Ligas disponibles</h1>

      {/*  Botones a cada liga  */}
      {loadingLeagues && <h2 className="loading-text">Cargando ligas...</h2>}
      {errorLeagues && <h2 className="error-text">{errorLeagues}</h2>}

      {!loadingLeagues && !errorLeagues && (
        <div className="buttons-container">
          {leagues.map((lg) => (
            <Link
              key={lg}
              to={`/league/${encodeURIComponent(lg)}`}
              className="league-button"
            >
              {lg}
            </Link>
          ))}
        </div>
      )}

      {/*  Tabla de resúmenes por liga y temporada  */}
      <h2 className="subtitle" style={{ marginTop: "2rem" }}>Resumen de ligas por temporada</h2>
      {loadingSummaries && <p className="loading-text">Cargando resúmenes...</p>}
      {errorSummaries && <p className="error-text">{errorSummaries}</p>}

      {!loadingSummaries && !errorSummaries && summaries.length > 0 && (
        <div className="table-wrapper">
          <table className="league-table">
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {summaries.map((row, idx) => (
                <tr key={`${row.liga}-${row.season}-${idx}`}>
                  {columns.map((c) => (
                    <td key={c} className={isNumber(c) ? "col-number" : ""}>{fmt(row[c])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
