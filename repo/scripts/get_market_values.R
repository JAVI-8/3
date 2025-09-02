# Establecer ruta de instalación local de paquetes
.libPaths("C:/Users/jahoy/Rlibs")

# Instalar dependencias si faltan
if (!require("devtools")) {
  install.packages("devtools", repos = "https://cloud.r-project.org", lib = .libPaths()[1])
}
if (!require("worldfootballR")) {
  devtools::install_github("JaseZiv/worldfootballR", lib = .libPaths()[1])
}
# Ruta de salida personalizada
output_dir <- "C:/Universidad/Master BDDE UCM/TFM/TFM/data/v1"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

library(worldfootballR)

# Definir ligas y temporadas
ligas <- list(
  Spain = "LaLiga",
  England = "Premier League",
  Italy = "Serie A",
  France = "Ligue 1",
  Germany = "Bundesliga"
)
args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(name, default = NULL) {
  m <- grep(paste0("^--", name, "="), args, value = TRUE)
  if (length(m)) sub(paste0("^--", name, "="), "", m[1]) else default
}

temporada <- get_arg("temporada", default = "2024-2025")
parts <- strsplit(temporada, "-", fixed = TRUE)[[1]]
anio_ini <- suppressWarnings(as.integer(parts[1]))

df_total <- data.frame() 

for (pais in names(ligas)) {
  message(paste0("Descargando: ", ligas[[pais]], " ", temporada))

  urls <- tryCatch(
    tm_league_team_urls(country_name = pais, start_year = anio_ini),
    error = function(e) { message("Error al obtener las URLs de equipos"); return(NULL) }
  )
  
  if (!is.null(urls)) {
    df_list <- lapply(urls, function(u) {
      tryCatch(tm_each_team_player_market_val(u), error = function(e) NULL)
    })
    df <- do.call(rbind, df_list)

    df$Season <- temporada
    df$Liga <- switch(ligas[[pais]],
                  "LaLiga" = "La-Liga",
                  "Premier League" = "Premier-League",
                  "Serie A" = "Serie-A",
                  "Ligue 1" = "Ligue-1",
                  "Bundesliga" = "Bundesliga")
    
    
    df_total <- rbind(df_total, df)
   
  } else{
    message("Error al obtener las URLs de equipos");
  }
  Sys.sleep(0.2)
}
file_name <- file.path(
        output_dir,
        paste0("valores_mercado3_", temporada, ".csv")
)
write.csv(df_total, file_name, row.names = FALSE)
  message(paste0("Archivo guardado: ", file_name))
