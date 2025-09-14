import tkinter as tk
from tkinter import messagebox
from controlador_scrpapping import run


def mi_funcion():
    run("2025-2026", False)
    messagebox.showinfo("Mensaje", "¡La función se ejecutó!")

root = tk.Tk()
root.title("App TFM")

# --- Pantalla completa (kiosco) ---
# Ocupa toda la pantalla y oculta bordes/controles de la ventana
root.attributes("-fullscreen", True)

# Si prefieres maximizar (manteniendo bordes), usa en Windows:
# root.state("zoomed")

# --- Contenido ---
contenedor = tk.Frame(root)
contenedor.pack(expand=True, fill="both")

btn = tk.Button(contenedor, text="Scrape y limpiar los datos", font=("Arial", 20), command=mi_funcion)
btn.pack(pady=20)

info = tk.Label(contenedor, text="F5: Salir  •  Esc: alternar pantalla completa", font=("Arial", 12))
info.pack(pady=10)

# --- Atajos de teclado ---
estado = {"fullscreen": True}

def alternar_fullscreen(event=None):
    estado["fullscreen"] = not estado["fullscreen"]
    root.attributes("-fullscreen", estado["fullscreen"])

def salir_fullscreen(event=None):
    estado["fullscreen"] = False
    root.attributes("-fullscreen", False)
    # Si en vez de salir de fullscreen quieres cerrar:
    # root.destroy()

root.bind("<Escape>", alternar_fullscreen)
root.bind("<F5>", salir_fullscreen)

root.mainloop()
