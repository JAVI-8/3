# Establecer ruta de instalación local de paquetes
.libPaths("C:/Users/jahoy/Rlibs")

# Instalar dependencias si faltan
if (!require("devtools")) {
  install.packages("devtools", repos = "https://cloud.r-project.org", lib = .libPaths()[1])
}
if (!require("worldfootballR")) {
  devtools::install_github("JaseZiv/worldfootballR", lib = .libPaths()[1])
}

library(worldfootballR)

# Ruta de salida personalizada
output_dir <- "C:/Universidad/Master BDDE UCM/TFM/TFM/prueba_data/mercado"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Definir ligas y temporadas
ligas <- list(
  #Germany = "Bundesliga",
  #Spain = "LaLiga",
  #England = "Premier League",
  #Italy = "Serie A"
  France = "Ligue 1"
)
anios <- 2022:2024

# Bucle principal
for (pais in names(ligas)) {
  for (anio in anios) {
    temporada_str <- paste0(anio, "-", anio + 1)
    liga_nombre <- ligas[[pais]]
    message(paste0("Descargando: ", liga_nombre, " ", temporada_str))

    urls <- tryCatch(
      tm_league_team_urls(country_name = pais, start_year = anio),
      error = function(e) { message("Error al obtener las URLs de equipos"); return(NULL) }
    )
    
    if (!is.null(urls)) {
      df_list <- lapply(urls, function(u) {
        tryCatch(tm_each_team_player_market_val(u), error = function(e) NULL)
      })
      df <- do.call(rbind, df_list)
      
      if (!is.null(df) && nrow(df) > 0) {
        df$temporada <- temporada_str
        df$liga <- liga_nombre

        file_name <- file.path(
          output_dir,
          paste0("valores_mercado_", gsub(" ", "-", liga_nombre), "_", temporada_str, ".csv")
        )

        write.csv(df, file_name, row.names = FALSE)
        message(paste0("Archivo guardado: ", file_name))
      } else {
        message("Sin datos válidos para esta combinación.")
      }
    }
  }
}
