# ===========================================
# Market values por jugador (Transfermarkt) con worldfootballR
# - Recorre temporadas 2019-2020 a 2025-2026
# - Ruta directa por país (evita tm_league_team_urls)
# - Normaliza columnas y filtra /profil/
# - Backoff + retries + User-Agent real
# - Dedup por jugador (última season)
# - Limpieza de tipos y opción de filtrar NAs en MV
# - Guardado CSV por temporada
# ===========================================

suppressPackageStartupMessages({
  library(worldfootballR)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(readr)
})

# -------- Parámetros fijos --------

# Carpeta base de salida
out_dir   <- "C:/Users/jahoy/Documents/scouting/lake/bronze/mercado"

# Si quieres limitar a un solo país:
# solo_pais <- "Spain"   # o "England", "Italy", "France", "Germany"
solo_pais <- NA         # NA = todos los países del mapa

# Si quieres eliminar filas sin market value:
drop_mv_na <- FALSE     # TRUE para filtrar NAs en player_market_value_euro

if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

ligas_map <- c(
  Spain   = "La-Liga",
  England = "Premier-League",
  Italy   = "Serie-A",
  France  = "Ligue-1",
  Germany = "Bundesliga"
)

paises <- names(ligas_map)
if (!is.na(solo_pais)) {
  if (!solo_pais %in% paises) stop("solo_pais debe ser uno de: ", paste(paises, collapse=", "))
  paises <- solo_pais
}

# -------- Opciones / utilidades --------
options("worldfootballR.user_agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

with_retry <- function(fn, tries = 6, wait_min = 1.2, wait_max = 2.5) {
  for (i in seq_len(tries)) {
    res <- tryCatch(fn(), error = function(e) e)
    if (!inherits(res, "error")) return(res)
    if (i < tries) Sys.sleep(runif(1, wait_min, wait_max))
  }
  stop(res)
}

# Columnas esperadas para alinear por nombre
req_cols <- c(
  "comp_name","region","country","season_start_year","squad","player_num",
  "player_name","player_position","player_dob","player_age","player_nationality",
  "current_club","player_height_mtrs","player_foot","date_joined","joined_from",
  "contract_expiry","player_market_value_euro","player_url"
)

normalize_df <- function(x) {
  if (!is.data.frame(x) || nrow(x) == 0) return(NULL)
  x <- tibble::as_tibble(x)
  miss <- setdiff(req_cols, names(x))
  if (length(miss)) x[miss] <- NA
  x <- x[, unique(c(req_cols, names(x)))]
  x
}

# Guardado robusto (evita locks en Windows y crea fallback con timestamp)
safe_write_csv <- function(df, path_target) {
  dir.create(dirname(path_target), recursive = TRUE, showWarnings = FALSE)
  ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  tmp <- paste0(path_target, ".tmp")
  fallback <- sub("\\.csv$", paste0("_", ts, ".csv"), path_target)
  tryCatch({
    readr::write_csv(df, tmp, na = "")
    ok <- file.rename(tmp, path_target)
    if (!ok) {
      readr::write_csv(df, fallback, na = "")
      message("[WARN] No pude sobrescribir '", basename(path_target), "'. Guardado como: ", basename(fallback))
      return(invisible(fallback))
    } else {
      message("Guardado: ", path_target)
      return(invisible(path_target))
    }
  }, error = function(e) {
    message("[WARN] Error al guardar: ", conditionMessage(e))
    if (file.exists(tmp)) unlink(tmp)
    readr::write_csv(df, fallback, na = "")
    message("[INFO] Guardado de respaldo: ", fallback)
    invisible(fallback)
  })
}

# ---- Extracción por país (ruta directa) ----
get_mv_by_country <- function(country_arg, start_year) {
  message(sprintf(">>> %s | %s-%s", country_arg, start_year, start_year + 1))
  Sys.sleep(runif(1, 1.0, 2.0))
  df_raw <- tryCatch(
    suppressWarnings(with_retry(function()
      tm_player_market_values(country_name = country_arg, start_year = start_year)
    )),
    error = function(e) { message("[WARN] ", country_arg, " -> ", e$message); NULL }
  )
  df_norm <- normalize_df(df_raw)
  if (is.null(df_norm)) {
    message("[WARN] 0 filas tras la llamada para ", country_arg)
    return(tibble())
  }
  liga_label <- if (country_arg %in% names(ligas_map)) ligas_map[[country_arg]] else country_arg
  df_norm %>%
    filter(!is.na(player_url), str_detect(player_url, "/profil/")) %>%  # solo perfiles
    filter(!is.na(player_name), player_name != "") %>%                  # nombre válido
    filter(!str_detect(player_name, "^€")) %>%                          # sanity extra
    mutate(
      Season = paste0(start_year, "-", start_year + 1),
      Liga   = liga_label
    )
}

# ===============================
# BUCLE DE TEMPORADAS 2019-2020 → 2025-2026
# ===============================

for (start_year in 2019:2025) {

  temporada <- paste0(start_year, "-", start_year + 1)

  message("\n=======================================")
  message(">>> PROCESANDO TEMPORADA: ", temporada)
  message("=======================================\n")

  # ---- Extracción principal para esta temporada ----
  df_total <- map_dfr(paises, ~ get_mv_by_country(.x, start_year))

  # Si quedó vacío, intenta temporada anterior para diagnóstico
  if (nrow(df_total) == 0) {
    message("[INFO] 0 filas en ", temporada, ". Intento temporada anterior…")
    df_prev <- map_dfr(paises, ~ get_mv_by_country(.x, start_year - 1))
    message("[INFO] Filas en temporada anterior: ", nrow(df_prev))
    if (nrow(df_prev) > 0) {
      message("[INFO] Uso temporada anterior como salida provisional para ", temporada, ".")
      df_total <- df_prev
      df_total$Season <- paste0(start_year, "-", start_year + 1)  # reetiquetamos la Season
    }
  }

  if (nrow(df_total) == 0) {
    message("[ERROR] Sin datos en todas las ligas para ", temporada, ". Paso a la siguiente temporada.")
    next
  }

  # ---- Deduplicación por jugador (última season si hubiera varias) ----
  df_total <- df_total %>%
    group_by(player_url) %>%
    slice_max(season_start_year, with_ties = FALSE) %>%
    ungroup()

  # ---- Limpieza de tipos + opcional filtrar NAs en MV ----
  df_total <- df_total %>%
    mutate(
      season_start_year        = suppressWarnings(as.integer(season_start_year)),
      player_market_value_euro = suppressWarnings(as.numeric(player_market_value_euro)),
      player_age               = suppressWarnings(as.integer(player_age)),
      player_height_mtrs       = suppressWarnings(as.numeric(player_height_mtrs))
    )

  if (isTRUE(drop_mv_na)) {
    df_total <- df_total %>% filter(!is.na(player_market_value_euro))
  }

  # ---- Resumen rápido ----
  message("Filas totales a guardar (", temporada, "): ", nrow(df_total))
  print(df_total %>% count(Liga, sort = TRUE))
  print(
    df_total %>%
      select(
        Liga,
        current_club = squad,
        player_name,
        player_position,
        player_market_value_euro
      ) %>%
      head(10)
  )

  # ---- Guardado ----
  csv_main <- file.path(out_dir, paste0("mercado_", temporada, "_raw.csv"))
  safe_write_csv(df_total, csv_main)
  message("guardado csv: ", csv_main)
}
