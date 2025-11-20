const API_URL = "http://127.0.0.1:8000";

export async function getPlayers(filters = {}) {
    const params = new URLSearchParams(filters);
    const res = await fetch(`${API_URL}/players?${params.toString()}`);
    return res.json();
}
