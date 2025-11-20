// src/pages/LeaguesPage.jsx
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import "./LeaguesPage.css";

export default function LeaguesPage() {
  const [leagues, setLeagues] = useState([]);

  useEffect(() => {
    async function loadLeagues() {
      const res = await fetch("http://127.0.0.1:8000/leagues");
      const data = await res.json();
      setLeagues(data);
    }
    loadLeagues();
  }, []);

  return (
    <div className="page-container">
      <h1 className="title">Ligas disponibles</h1>

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
    </div>
  );
}
