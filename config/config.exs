use Mix.Config

config :triton,
  clusters: [
    []
  ],
  enable_dual_writes: false,
  apm_module: Triton.APM.Noop
