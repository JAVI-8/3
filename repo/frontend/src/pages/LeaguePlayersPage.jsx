// src/pages/LeaguePlayersPage.jsx
import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import "./LeaguePlayersPage.css";

const API_BASE = "http://127.0.0.1:8000";

export default function LeaguePlayersPage() {
  const { league } = useParams();
  const [players, setPlayers] = useState([]);
  const [leagues, setLeagues] = useState([]);
  const [seasons, setSeasons] = useState([]);
  const [selectedSeason, setSelectedSeason] = useState("");
  const [loading, setLoading] = useState(true);

  // 1) Cargar lista de ligas (para los botones de arriba)
  useEffect(() => {
    async function loadLeagues() {
      const res = await fetch(`${API_BASE}/leagues`);
      const data = await res.json();
      setLeagues(data);
    }
    loadLeagues();
  }, []);

  // 2) Cargar temporadas de la liga (para la columna izquierda)
  useEffect(() => {
    async function loadSeasons() {
      const res = await fetch(
        `${API_BASE}/league/${encodeURIComponent(league)}/season_summary`
      );
      const data = await res.json(); // [{Season: "...", ...}, ...]
      const uniqueSeasons = [...new Set(data.map((row) => row.Season))];
      setSeasons(uniqueSeasons);

      // opcional: seleccionar la primera temporada por defecto
      if (uniqueSeasons.length && !selectedSeason) {
        setSelectedSeason(uniqueSeasons[0]);
      }
    }
    loadSeasons();
  }, [league]);

  // 3) Cargar jugadores filtrados por liga + temporada
  useEffect(() => {
    async function loadPlayers() {
      setLoading(true);
      let url = `${API_BASE}/players?league=${encodeURIComponent(
        league
      )}&limit=300`;

      if (selectedSeason) {
        url += `&season=${encodeURIComponent(selectedSeason)}`;
      }

      const res = await fetch(url);
      const data = await res.json();
      setPlayers(data);
      setLoading(false);
    }
    loadPlayers();
  }, [league, selectedSeason]);

  if (loading || !players.length) {
    return <h2 style={{ padding: "2rem" }}>Cargando jugadores...</h2>;
  }

  const cols = Object.keys(players[0] || {});
  const numericCols = [
    "minutesPlayed",
    "goals",
    "assists",
    "player_market_value_euro",
  ];

  return (
    <div className="page-league">
      <Link to="/" className="back-link">
        ← Volver a ligas
      </Link>

      <h1>Jugadores – {decodeURIComponent(league)}</h1>

      {/* 🔵 Botones de ligas arriba */}
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

      {/* 📐 Layout: izquierda temporadas, derecha tabla */}
      <div className="content-layout">
        {/* Columna izquierda: temporadas */}
        <aside className="sidebar">
          <h2 className="sidebar-title">Temporadas</h2>
          <div className="seasons-column">
            {seasons.map((season) => (
              <button
                key={season}
                onClick={() => setSelectedSeason(season)}
                className={`season-button ${
                  season === selectedSeason ? "active" : ""
                }`}
              >
                {season}
              </button>
            ))}
          </div>
        </aside>

        {/* Parte derecha: tabla */}
        <div className="table-wrapper">
          <table className="league-table">
            <thead>
              <tr>
                {cols.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {players.map((p, i) => (
                <tr key={i}>
                  {cols.map((c) => (
                    <td
                      key={c}
                      className={numericCols.includes(c) ? "col-number" : ""}
                    >
                      {String(p[c] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
