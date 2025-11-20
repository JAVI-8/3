// src/App.jsx
import { Routes, Route } from "react-router-dom";
import LeagueListPage from "./pages/LeagueListPage";
import LeaguePlayersPage from "./pages/LeaguePlayersPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<LeagueListPage />} />
      <Route path="/league/:league" element={<LeaguePlayersPage />} />
    </Routes>
  );
}
