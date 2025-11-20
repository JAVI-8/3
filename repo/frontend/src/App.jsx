// src/App.jsx
import { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchPlayers() {
      try {
        const res = await fetch("http://127.0.0.1:8000/players?limit=200");
        const data = await res.json();
        setPlayers(data);
      } catch (err) {
        console.error("Error cargando jugadores:", err);
      } finally {
        setLoading(false);
      }
    }

    fetchPlayers();
  }, []);

  if (loading) {
    return <div className="App"><h2>Cargando jugadores...</h2></div>;
  }

  if (!players.length) {
    return <div className="App"><h2>No se han encontrado jugadores</h2></div>;
  }

  // columnas dinámicas para lo que devuelva el backend
  const columns = Object.keys(players[0]);

  return (
    <div className="App">
      <h1>Tabla GOLD – Players + Market Value</h1>
      <table>
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {players.map((row, i) => (
            <tr key={i}>
              {columns.map((col) => (
                <td key={col}>
                  {col === "player_market_value_euro" && row[col] != null
                    ? row[col].toLocaleString("es-ES", {
                        maximumFractionDigits: 0,
                      })
                    : row[col]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
