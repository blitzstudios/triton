use Mix.Config

config :triton,
  clusters: [
    []
  ],
  enable_dual_writes: false,
  disable_validation: false,
  apm_module: Triton.APM.Noop

