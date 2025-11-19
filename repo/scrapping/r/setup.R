lib <- "C:/Users/jahoy/Rlibs"  # usa / o \\ (evita \ suelta)
dir.create(lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(lib, .libPaths()))

install.packages(
  c("worldfootballR"),
  repos = "https://cloud.r-project.org",
  lib = lib
)

cat("OK. libPaths:\n"); print(.libPaths())
