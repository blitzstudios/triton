use Mix.Config

config :triton,
  clusters: [
    []
  ],
  enable_dual_writes: false,
  enable_dual_reads: false,
  enable_auto_prepare: false,
  disable_validation: false,
  disable_compilation_migrations: false,
  apm_module: Triton.APM.Noop

import_config "#{Mix.env}.exs"

