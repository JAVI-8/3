#imports
import os
import threading
import traceback
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
import pandas as pd


#  configuración
PARQUET_PATH = Path(os.environ.get(
    "PARQUET_PATH",
    r"work/parquets/latest/top.parquet"#ruta del archivo a mostrar
))

MAX_ROWS = 2000  # filas max a mostrar en la tabla



scrap_fbref_and_clean = run_r = None
procesar = calcular = None

try:
    from controlador_scrpapping import scrap_fbref_and_clean, run_r
except Exception:
    
    try:
        from controlador_scrpapping import scrap_fbref_and_clean, run_r
    except Exception as e:
        print(f"[AVISO] No pude importar run/run_r: {e}")

# Spark pipeline
try:
    from spark_pipeline.procesamiento_spark import procesar
    from spark_pipeline.scoring_utils import calcular
except Exception as e:
    print(f"[AVISO] No pude importar procesar/calcular: {e}")
    


#  THEME (ttk, oscuro + acento)
def _clamp(x): return max(0, min(255, x))
def _hex_to_rgb(h): h = h.lstrip("#"); return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
def _rgb_to_hex(c): return "#{:02X}{:02X}{:02X}".format(*c)
def _tint(hexcolor, delta):
    """Aclara/oscurece un color hex (delta 0.1 ≈ +10%, -0.1 ≈ -10%)."""
    r, g, b = _hex_to_rgb(hexcolor)
    if delta >= 0:
        r = _clamp(int(r + (255 - r) * delta))
        g = _clamp(int(g + (255 - g) * delta))
        b = _clamp(int(b + (255 - b) * delta))
    else:
        r = _clamp(int(r * (1 + delta)))
        g = _clamp(int(g * (1 + delta)))
        b = _clamp(int(b * (1 + delta)))
    return _rgb_to_hex((r, g, b))

def aplicar_tema(root, accent="#22C55E"):
    bg     = "#0F172A"  # fondo
    panel  = "#111827"  # barras/paneles
    text   = "#E5E7EB"  # texto normal
    muted  = "#9CA3AF"  # texto secundario
    border = "#1F2937"  # bordes
    sel_bg = "#064E3B"  # selección fila
    sel_fg = "#E5E7EB"

    style = ttk.Style(root)
    style.theme_use("clam")

    style.configure(".", background=bg, foreground=text, borderwidth=0)
    style.configure("TFrame", background=bg)
    style.configure("Panel.TFrame", background=panel)
    style.configure("TLabel", background=bg, foreground=text, font=("Segoe UI", 10))
    style.configure("Muted.TLabel", background=bg, foreground=muted)

    style.configure("Accent.TButton",
                    background=accent, foreground="#0B0F19",
                    padding=10, focusthickness=3, focuscolor=border)
    style.map("Accent.TButton",
              background=[("active", _tint(accent, 0.1)),
                          ("pressed", _tint(accent, -0.1))])

    style.configure("TButton",
                    background=panel, foreground=text, padding=10, relief="flat")
    style.map("TButton",
              background=[("active", "#1F2937"), ("pressed", "#0B1220")])

    style.configure("Vertical.TScrollbar", background=panel, troughcolor=bg)
    style.configure("Horizontal.TScrollbar", background=panel, troughcolor=bg)

    style.configure("Treeview",
                    background=bg, fieldbackground=bg, foreground=text,
                    rowheight=26, bordercolor=border, bordersize=1)
    style.map("Treeview",
              background=[("selected", sel_bg)],
              foreground=[("selected", sel_fg)])
    style.configure("Treeview.Heading",
                    background=panel, foreground=text, relief="flat",
                    font=("Segoe UI Semibold", 10))
    style.map("Treeview.Heading", background=[("active", panel)])

def aplicar_cebra(tree: ttk.Treeview, alt="#0B1220"):
    tree.tag_configure("oddrow", background=alt)
    for i, iid in enumerate(tree.get_children("")):
        if i % 2:
            tree.item(iid, tags=("oddrow",))

#  leer el archivo

def read_parquet_fixed() -> pd.DataFrame:
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"No existe la ruta: {PARQUET_PATH}")
    # pandas/pyarrow soporta carpeta parquet o archivo .parquet
    return pd.read_parquet(str(PARQUET_PATH), engine="pyarrow")


# tabla

def create_table(parent: tk.Widget) -> ttk.Treeview:
    container = ttk.Frame(parent)
    container.pack(expand=True, fill="both")

    vsb = ttk.Scrollbar(container, orient="vertical")
    hsb = ttk.Scrollbar(container, orient="horizontal")

    tree = ttk.Treeview(
        container,
        columns=[], show="headings",
        yscrollcommand=vsb.set,
        xscrollcommand=hsb.set
    )
    vsb.config(command=tree.yview)
    hsb.config(command=tree.xview)

    container.rowconfigure(0, weight=1)
    container.columnconfigure(0, weight=1)

    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    return tree

def populate_table(tree: ttk.Treeview, df: pd.DataFrame):
    tree.delete(*tree.get_children())
    df_show = df.head(MAX_ROWS).copy()

    cols = list(map(str, df_show.columns))
    tree["columns"] = cols
    for c in cols:
        tree.heading(c, text=c)
        try:
            avg_len = int(df_show[c].astype(str).str.len().head(200).mean() * 9)
        except Exception:
            avg_len = 100
        tree.column(c, width=min(240, max(80, avg_len)))

    rows = df_show.astype(object).where(pd.notna(df_show), None).values.tolist()
    for r in rows:
        tree.insert("", "end", values=r)

    aplicar_cebra(tree)


def run_async(fn, on_ok=None, on_err=None):
    def worker():
        try:
            res = fn()
        except Exception as e:
            if on_err: root.after(0, lambda m=str(e): on_err(m))
        else:
            if on_ok: root.after(0, lambda: on_ok(res))
    threading.Thread(target=worker, daemon=True).start()

busy = {"value": False}

def run_chain(steps, success_msg, after_ok=None):
    """steps: lista de pasos; acepta (fn, args, kwargs) | (fn, args) | fn"""
    if busy["value"]:
        messagebox.showinfo("Aviso", "Ya hay un proceso en ejecución.")
        return

    busy["value"] = True
    btn_fbref.config(state="disabled")
    btn_tmkt.config(state="disabled")
    btn_merge.config(state="disabled")
    btn_score.config(state="disabled")
    
    def worker():
        try:
            for step in steps:
                if callable(step):
                    fn, args, kwargs = step, tuple(), {}
                elif isinstance(step, (list, tuple)):
                    if len(step) == 3:
                        fn, args, kwargs = step
                    elif len(step) == 2:
                        fn, args = step; kwargs = {}
                    elif len(step) == 1:
                        fn, args, kwargs = step[0], tuple(), {}
                    else:
                        raise ValueError(f"Paso inválido: {step}")
                else:
                    raise ValueError(f"Paso inválido: {step}")
                fn(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            err = f"Ocurrió un error:\n{e}"
            root.after(0, lambda m=err: messagebox.showerror("Error", m))
        else:
            if after_ok: root.after(0, after_ok)
            root.after(0, lambda m=success_msg: messagebox.showinfo("Éxito", m))
        finally:
            busy["value"] = False
            root.after(0, lambda: (
                btn_fbref.config(state="normal"),
                btn_tmkt.config(state="normal"),
                btn_merge.config(state="normal"),
                btn_score.config(state="normal")
            ))
    threading.Thread(target=worker, daemon=True).start()

# acciones de los botones
def accion_fbref(): #obtener csvs de fbrf
    if scrap_fbref_and_clean is None:
        messagebox.showinfo("FBref", "Conecta aquí run(season, ejecutar_r=False).")
        return
    run_chain(
        steps=[(scrap_fbref_and_clean, ("2024-2025",), {})],
        success_msg="FBref: scrape + limpieza completados.",
        after_ok=recargar
    )

def accion_transfermarkt():#obtener csv mercado de transfermarkt
    if run_r is None:
        messagebox.showinfo("Transfermarkt", "Conecta aquí run_r(temporada) + limpiar_mercado(temporada).")
        return
    temporada = "2024-2025"
    run_chain(
        steps=[(run_r, (temporada,), {})],
        success_msg="Transfermarkt: scrape + limpieza completados.",
        after_ok=recargar
    )

def accion_merge(): #para unir los csvs
    if procesar is None:
        messagebox.showinfo("Transfermarkt", "Conecta aquí run_r(temporada) + limpiar_mercado(temporada).")
        return
    output = "work/parquets/latest"
    input = "data/v2"
    run_chain(
        steps=[(procesar, (input, output), {})],
        success_msg="merge completado.",
        after_ok=recargar
    )
    
def accion_score():#calcular el score de cada jugador
    if calcular is None:
        messagebox.showinfo("Transfermarkt", "Conecta aquí run_r(temporada) + limpiar_mercado(temporada).")
        return
    output = "work/parquets/latest"
    input = "work/parquets/latest/players.parquet"
    run_chain(
        steps=[(calcular, (input, output), {})],
        success_msg="score completado.",
        after_ok=recargar
    )

def recargar():
    def task(): return read_parquet_fixed()
    def on_ok(df): populate_table(table, df)
    def on_err(msg): messagebox.showerror("Error al cargar Parquet", f"{msg}\n\nRuta: {PARQUET_PATH}")
    run_async(task, on_ok, on_err)

root = tk.Tk()
root.title("App TFM")
root.geometry("1200x800")

aplicar_tema(root)

#topbar
topbar = ttk.Frame(root, style="Panel.TFrame", padding=8)
topbar.pack(side="top", fill="x")

btn_fbref = ttk.Button(topbar, text="FBref: Scrape + Limpiar", style="Accent.TButton", command=accion_fbref)
btn_tmkt  = ttk.Button(topbar, text="Transfermarkt: Scrape + Limpiar", command=accion_transfermarkt)
btn_merge = ttk.Button(topbar, text="Procesar csvs", style="Accent.TButton", command=accion_merge)
btn_score  = ttk.Button(topbar, text="calcular puntuación", command=accion_score)
btn_fbref.pack(side="left", padx=6)
btn_tmkt.pack(side="left", padx=6)
btn_merge.pack(side="left", padx=6)
btn_score.pack(side="left", padx=6)
info = ttk.Label(topbar, style="Muted.TLabel",
                 text=f"F5: recargar  •  Esc: pantalla completa  •  Origen: {PARQUET_PATH}")
info.pack(side="right", padx=6)
#tabla
table = create_table(root)

#teclas de atajo
def alternar_fullscreen(_=None):
    root.attributes("-fullscreen", not bool(root.attributes("-fullscreen")))
def salir_app(_=None):
    root.destroy()

root.bind("<F5>", lambda e: recargar())
root.bind("<Escape>", alternar_fullscreen)
root.bind("<Control-q>", salir_app)

#primera carga
recargar()

root.mainloop()
