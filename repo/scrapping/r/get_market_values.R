# ===========================================
# Market values por jugador (Transfermarkt) con worldfootballR
# - Temporadas 2019-2020 a 2025-2026
# - Ruta directa por país (tm_player_market_values)
# - Normaliza columnas sin castear tipos
# - Filtra /profil/ + filtro anti datos corruptos
# - Guarda UN CSV por LIGA y TEMPORADA:
#   mercado_<Liga>_<Season>_raw.csv
# ===========================================

suppressPackageStartupMessages({
  library(worldfootballR)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(readr)
})

# -------- Parámetros fijos --------

out_dir <- "C:/Users/jahoy/Documents/scouting/lake/bronze/mercado"

solo_pais <- NA  # NA = todos los países

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
options("worldfootballR.user_agent" =
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

with_retry <- function(fn, tries = 6, wait_min = 1.2, wait_max = 2.5) {
  for (i in seq_len(tries)) {
    res <- tryCatch(fn(), error = function(e) e)
    if (!inherits(res, "error")) return(res)
    if (i < tries) Sys.sleep(runif(1, wait_min, wait_max))
  }
  stop(res)
}

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
      message("[WARN] No pude sobrescribir '", basename(path_target),
              "'. Guardado como: ", basename(fallback))
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

# ---- Función principal por país/temporada ----
get_mv_by_country <- function(country_arg, start_year) {
  message(sprintf(">>> %s | %s-%s", country_arg, start_year, start_year + 1))
  Sys.sleep(runif(1, 1.0, 2.0))

  df_raw <- tryCatch(
    suppressWarnings(with_retry(function()
      tm_player_market_values(country_name = country_arg, start_year = start_year)
    )),
    error = function(e) {
      message("[WARN] ", country_arg, " -> ", e$message)
      NULL
    }
  )

  df_norm <- normalize_df(df_raw)
  if (is.null(df_norm)) {
    message("[WARN] 0 filas tras la llamada para ", country_arg)
    return(tibble())
  }

  liga_label <- if (country_arg %in% names(ligas_map)) ligas_map[[country_arg]] else country_arg

  # Filtros + FILTRO ANTI DATOS CORRUPTOS
  df_norm %>%
    filter(!is.na(player_url), str_detect(player_url, "/profil/")) %>%
    filter(!is.na(player_name), player_name != "") %>%
    filter(!str_detect(player_name, "^€")) %>%
    # <<<<< FILTRO CRÍTICO >>>>>
    # elimina registros corruptos donde current_club ≠ squad
    filter(is.na(current_club) | is.na(squad) | current_club == squad) %>%
    mutate(
      Season = paste0(start_year, "-", start_year + 1),
      Liga   = liga_label
    )
}

# ===============================
#    LOOP PRINCIPAL
# ===============================

for (start_year in 2019:2025) {

  temporada <- paste0(start_year, "-", start_year + 1)

  message("\n=======================================")
  message(">>> PROCESANDO TEMPORADA: ", temporada)
  message("=======================================\n")

  for (country_arg in paises) {

    liga_label <- ligas_map[[country_arg]]

    message("\n--- Liga: ", liga_label, " (", country_arg, ") ---")

    df_liga <- get_mv_by_country(country_arg, start_year)

    # Intento temporada anterior si está vacío
    if (nrow(df_liga) == 0) {
      message("[INFO] 0 filas en ", temporada, ". Intento temporada anterior…")
      df_prev <- get_mv_by_country(country_arg, start_year - 1)
      message("[INFO] Filas en temporada anterior: ", nrow(df_prev))
      if (nrow(df_prev) > 0) {
        message("[INFO] Uso temporada anterior como salida provisional para ",
                liga_label, " en ", temporada, ".")
        df_liga <- df_prev
        df_liga$Season <- temporada
      }
    }

    if (nrow(df_liga) == 0) {
      message("[ERROR] Sin datos para ", liga_label, " en ", temporada, ". Siguiente.")
      next
    }

    # Deduplicación por jugador
    df_liga <- df_liga %>%
      group_by(player_url) %>%
      slice_max(season_start_year, with_ties = FALSE) %>%
      ungroup()

    message("Filas a guardar (", liga_label, ", ", temporada, "): ", nrow(df_liga))

    csv_main <- file.path(
      out_dir,
      paste0("mercado_", liga_label, "_", temporada, "_raw.csv")
    )

    safe_write_csv(df_liga, csv_main)
    message("Guardado CSV: ", csv_main)
  }
}
